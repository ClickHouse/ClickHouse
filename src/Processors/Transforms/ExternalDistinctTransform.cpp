#include <Processors/Transforms/ExternalDistinctTransform.h>

#include <algorithm>
#include <type_traits>

#include <Interpreters/sortBlock.h>
#include <Processors/Merges/DistinctSortedTransform.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>

namespace ProfileEvents
{
    extern const Event ExternalDistinctMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char external_distinct_suppression_run_prepared_pause[];
}

namespace
{

/// Suppression extraction targets this many bytes per run, independently of the spill threshold.
/// Ordinary runs use the smaller of this value and the threshold as their minimum accumulated size.
constexpr size_t DEFAULT_BYTES_IN_RUN = DEFAULT_BLOCK_SIZE * 256;

}

ExternalDistinctTransform::ExternalDistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    size_t max_bytes_before_external_distinct_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_,
    size_t max_block_size_rows_,
    bool preserve_input_order_)
    : IProcessor({header_}, {header_})
    , state(std::in_place_type<Hashing>, *header_, columns_, set_size_limits_)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , max_bytes_before_external_distinct(max_bytes_before_external_distinct_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_size_rows(max_block_size_rows_)
    , spill_layout(header_, std::get<Hashing>(state).set.getKeyColumnsPositions(), preserve_input_order_)
{
}

ExternalDistinctTransform::~ExternalDistinctTransform() = default;

size_t ExternalDistinctTransform::minBytesInRun() const
{
    return std::min(max_bytes_before_external_distinct, DEFAULT_BYTES_IN_RUN);
}

IProcessor::Status ExternalDistinctTransform::prepare()
{
    if (outputs.front().isFinished())
    {
        /// Closing every dependency lets active sinks and readers terminate. Their resources remain
        /// owned by the pipeline until destruction, outside the executor's preparation lock.
        return finish();
    }

    return std::visit([this]<typename Phase>(Phase & phase) -> Status
    {
        if constexpr (std::is_same_v<Phase, Hashing>)
        {
            auto status = prepareInput();
            if (status == Status::Finished)
            {
                phase.input_finished = true;
                return Status::Ready;
            }
            return status;
        }
        else if constexpr (std::is_same_v<Phase, CollectingInput>)
            return prepareCollectingInput(phase);
        else if constexpr (std::is_same_v<Phase, ExtractingSuppression> || std::is_same_v<Phase, PreparingTail>)
            return Status::Ready;
        else if constexpr (std::is_same_v<Phase, ConnectingSuppressionRun>
            || std::is_same_v<Phase, ConnectingInputRun> || std::is_same_v<Phase, ConnectingTail>)
            return Status::UpdatePipeline;
        else if constexpr (std::is_same_v<Phase, WritingSuppressionRun>)
            return prepareSuppressionWrite(phase);
        else if constexpr (std::is_same_v<Phase, WritingInputRun>)
            return prepareInputWrite(phase);
        else if constexpr (std::is_same_v<Phase, Merging>)
            return prepareMergedOutput(phase);
        else if constexpr (std::is_same_v<Phase, Finishing>)
            return prepareFinish();
    }, state);
}

IProcessor::Status ExternalDistinctTransform::prepareInput()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// A pending hashing result precedes results from the merged runs. Pulling one more input after
    /// pushing it preserves the separate capacities of the output port and the pending result slot.
    if (output_chunk)
        output.push(std::move(output_chunk));

    if (!input_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        input_chunk = input.pull(true);
    }

    return Status::Ready;
}

IProcessor::Status ExternalDistinctTransform::prepareCollectingInput(CollectingInput & collecting)
{
    auto status = prepareInput();
    if (status == Status::Finished)
    {
        auto chunks = std::move(collecting.chunks);
        state.emplace<PreparingTail>(std::move(chunks));
        return Status::Ready;
    }
    return status;
}

IProcessor::Status ExternalDistinctTransform::prepareRunWrite(RunWriteProgress & progress, OutputPort & output)
{
    if (!output.isFinished())
    {
        if (!output.canPush())
            return Status::PortFull;

        if (progress.chunk)
            output.push(std::move(progress.chunk));

        if (progress.merger)
            return Status::Ready;

        output.finish();
    }

    chassert(!progress.merger);
    chassert(!progress.chunk);
    return Status::Finished;
}

IProcessor::Status ExternalDistinctTransform::prepareSuppressionWrite(WritingSuppressionRun & writing)
{
    auto status = prepareRunWrite(writing.progress, writing.output);
    if (status != Status::Finished)
        return status;

    if (writing.completion.hasData())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected data on the external DISTINCT run completion port");

    if (!writing.completion.isFinished())
    {
        writing.completion.setNeeded();
        return Status::NeedData;
    }

    writing.readiness.finish();
    auto keys = std::move(writing.keys);
    state.emplace<ExtractingSuppression>(std::move(keys));
    return Status::Ready;
}

IProcessor::Status ExternalDistinctTransform::prepareInputWrite(WritingInputRun & writing)
{
    auto status = prepareRunWrite(writing.progress, writing.output);
    if (status != Status::Finished)
        return status;

    auto & collecting = state.emplace<CollectingInput>();
    /// Producer completion is sufficient here; the file reader waits for the sink independently.
    return prepareCollectingInput(collecting);
}

IProcessor::Status ExternalDistinctTransform::prepareMergedOutput(Merging & merging)
{
    auto & output = outputs.front();
    if (!output.canPush())
        return Status::PortFull;

    if (output_chunk)
        output.push(std::move(output_chunk));

    if (merging.input.isFinished())
    {
        state.emplace<Finishing>();
        return prepareFinish();
    }

    merging.input.setNeeded();
    if (!merging.input.hasData())
        return Status::NeedData;

    merging.chunk = merging.input.pull(true);
    return Status::Ready;
}

IProcessor::Status ExternalDistinctTransform::prepareFinish()
{
    auto & output = outputs.front();
    if (output_chunk)
    {
        if (!output.canPush())
            return Status::PortFull;

        output.push(std::move(output_chunk));
    }

    return finish();
}

IProcessor::Status ExternalDistinctTransform::finish()
{
    if (merge_registration)
    {
        /// An idle merger must leave registration before it can observe its closed output.
        merge_registration->merger->setHaveAllInputs();
        merge_registration.reset();
    }

    for (auto & input : inputs)
        input.close();
    for (auto & output : outputs)
        output.finish();
    return Status::Finished;
}

void ExternalDistinctTransform::work()
{
    std::visit([this]<typename Phase>(Phase & phase)
    {
        if constexpr (std::is_same_v<Phase, Hashing>)
        {
            if (phase.input_finished)
                state.emplace<Finishing>();
            else
                consumeHashing(phase);
        }
        else if constexpr (std::is_same_v<Phase, ExtractingSuppression>)
            extractSuppressionRun(phase);
        else if constexpr (std::is_same_v<Phase, CollectingInput>)
            collectInput(phase);
        else if constexpr (std::is_same_v<Phase, WritingSuppressionRun> || std::is_same_v<Phase, WritingInputRun>)
            readRun(phase.progress);
        else if constexpr (std::is_same_v<Phase, PreparingTail>)
            prepareTail(phase);
        else if constexpr (std::is_same_v<Phase, Merging>)
            consumeMerged(phase);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "External DISTINCT has no work in state {}", state.index());
    }, state);
}

void ExternalDistinctTransform::consumeHashing(Hashing & hashing)
{
    if (unlikely(!input_chunk.hasRows()))
    {
        input_chunk.clear();
        return;
    }

    /// Filtering can copy the input before spilling, so allow another input-sized allocation.
    /// A suppression run needs its columns, a sorted copy, and a permutation. Writing needs the
    /// uncompressed, compressed, and file buffers. Oversized values and codec overhead can exceed
    /// this estimate.
    const size_t suppression_columns_bytes = 2 * DEFAULT_BYTES_IN_RUN;
    const size_t sort_permutation_bytes = max_block_size_rows * sizeof(IColumn::Permutation::value_type);
    const size_t write_buffers_bytes = 3 * tmp_data->getSettings().buffer_size;
    const size_t spill_headroom_bytes
        = input_chunk.allocatedBytes() + suppression_columns_bytes + sort_permutation_bytes + write_buffers_bytes;
    hashing.set.prepareForInsert(input_chunk);
    if (const auto available = getMostStrictAvailableSystemMemory())
    {
        const size_t growth_memory = hashing.set.estimateGrowthMemory(input_chunk.getNumRows());
        if (growth_memory && (spill_headroom_bytes > *available || growth_memory > *available - spill_headroom_bytes))
        {
            startSpilling(hashing);
            return;
        }
    }

    consumed_rows += input_chunk.getNumRows();
    chassert(!output_chunk);
    output_chunk = hashing.set.filter(std::move(input_chunk));
    result_rows += output_chunk.getNumRows();

    /// A hint or a size limit in the 'break' overflow mode retains this final result chunk.
    if ((limit_hint && result_rows >= limit_hint) || hashing.set.isLimitReached())
    {
        state.emplace<Finishing>();
        return;
    }

    if (getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct))
        startSpilling(hashing);
}

void ExternalDistinctTransform::startSpilling(Hashing & hashing)
{
    LOG_TRACE(log, "Switching DISTINCT to the external mode (query memory: {}, spill threshold: {})",
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    if (hashing.set.getTotalRowCount())
    {
        auto keys = std::move(hashing.set).extractKeys();
        auto & extracting = state.emplace<ExtractingSuppression>(std::move(keys));
        extractSuppressionRun(extracting);
    }
    else
        state.emplace<CollectingInput>();
}

void ExternalDistinctTransform::extractSuppressionRun(ExtractingSuppression & extracting)
{
    Chunks chunks;
    size_t bytes = 0;

    /// Bound working columns to one run while the extractor retains the set and arena. A complete
    /// key can exceed the byte target, and sorting needs additional temporary buffers.
    while (!isCancelled() && bytes < DEFAULT_BYTES_IN_RUN)
    {
        auto key_columns = extracting.keys->next(max_block_size_rows, DEFAULT_BYTES_IN_RUN - bytes);
        if (key_columns.empty())
            break;

        auto chunk = spill_layout.prepareSuppressionChunk(std::move(key_columns));
        Block block = spill_layout.getSpillHeader()->cloneWithColumns(chunk.detachColumns());
        /// Stable sorting preserves binary representatives of sort-equivalent keys. The flag is
        /// constant within this chunk, so key order also satisfies the run order.
        sortBlock(block, spill_layout.getKeySortDescription(), /*limit=*/ 0, IColumn::PermutationSortStability::Stable);
        const auto rows = block.rows();
        Chunk sorted(block.detachColumns(), rows);
        bytes += sorted.allocatedBytes();
        chunks.push_back(std::move(sorted));
    }

    if (isCancelled())
        return;

    if (chunks.empty())
    {
        state.emplace<CollectingInput>();
        return;
    }

    auto run = prepareRun(std::move(chunks), bytes, spill_layout.getRunSortDescription(), MergeSorter::Mode::PreserveRows);
    auto keys = std::move(extracting.keys);
    auto & connecting = state.emplace<ConnectingSuppressionRun>(std::move(run), std::move(keys));
    FailPointInjection::pauseFailPoint(FailPoints::external_distinct_suppression_run_prepared_pause);
    readRun(connecting.run.progress);
}

void ExternalDistinctTransform::collectInput(CollectingInput & collecting)
{
    auto chunk = std::move(input_chunk);
    if (unlikely(!chunk.hasRows()))
        return;

    const UInt64 first_arrival_number = consumed_rows;
    consumed_rows += chunk.getNumRows();
    auto prepared = spill_layout.prepareInputChunk(std::move(chunk), first_arrival_number);
    Block block = spill_layout.getSpillHeader()->cloneWithColumns(prepared.detachColumns());
    /// Stable compaction keeps the first payload and permutes the service columns with its row.
    sortBlockAndDeduplicate(block, spill_layout.getKeySortDescription(), IColumn::PermutationSortStability::Stable);
    const auto rows = block.rows();
    Chunk sorted(block.detachColumns(), rows);
    collecting.bytes += sorted.allocatedBytes();
    collecting.chunks.push_back(std::move(sorted));

    /// An empty hash set produces no suppression files, so the first ordinary chunk starts a run.
    /// Later runs have a size floor when other operators keep query memory above the threshold.
    if (temporary_files_num == 0 || (collecting.bytes >= minBytesInRun()
        && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct)))
    {
        auto run = prepareRun(std::move(collecting.chunks), collecting.bytes,
            spill_layout.getKeySortDescription(), MergeSorter::Mode::MergeUniqueChunks);
        auto & connecting = state.emplace<ConnectingInputRun>(std::move(run));
        readRun(connecting.run.progress);
    }
}

ExternalDistinctTransform::PreparedRun ExternalDistinctTransform::prepareRun(
    Chunks chunks, size_t bytes, const SortDescription & description, MergeSorter::Mode mode)
{
    const auto & spill_header = spill_layout.getSpillHeader();
    ++temporary_files_num;

    LOG_TRACE(log, "Will dump distinct run ({} chunks, {}) to disk (query memory: {}, limit: {})",
        chunks.size(),
        formatReadableSizeWithBinarySuffix(bytes),
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    /// Reserving the run's space also preserves the configured amount of free disk space.
    TemporaryBlockStreamHolder tmp_stream(spill_header, tmp_data, bytes + min_free_disk_space);
    /// The final merge applies the hint after suppression, which can remove keys from ordinary runs.
    auto merger = std::make_unique<MergeSorter>(
        spill_header, std::move(chunks), description, max_block_size_rows, /*limit=*/ 0, mode);
    auto sink = std::make_shared<BufferingToFileSink>(spill_header, std::move(tmp_stream), log);
    auto source = std::make_shared<BufferingFromFileSource>(spill_header, sink->getHolder(), log);
    PreparedRun run{
        .progress = {std::move(merger), {}},
        .sink = std::move(sink),
        .source = std::move(source),
        .initial_merge = {},
    };
    if (!merge_registration)
        run.initial_merge = prepareMerge();

    return run;
}

void ExternalDistinctTransform::readRun(RunWriteProgress & progress)
{
    chassert(progress.merger);
    chassert(!progress.chunk);
    /// Reads can consume only duplicates and return zero rows. Skip those chunks while retaining
    /// cancellation checks between reads, and finish writing only when the merger is exhausted.
    while (!isCancelled())
    {
        progress.chunk = progress.merger->read();
        if (!progress.chunk)
            break;

        if (progress.chunk.hasRows())
            return;
    }

    progress.merger.reset();
}

void ExternalDistinctTransform::prepareTail(PreparingTail & tail)
{
    ProfileEvents::increment(ProfileEvents::ExternalDistinctMerge);
    LOG_INFO(log, "There are {} temporary distinct runs to merge", temporary_files_num);

    /// Register the final input even when the tail is empty, then close merge-input registration.
    /// The tail is merged into unique chunks under the same contract as ordinary disk runs.
    auto source = std::make_shared<MergeSorterSource>(
        spill_layout.getSpillHeader(), std::move(tail.chunks), spill_layout.getKeySortDescription(),
        max_block_size_rows, /*limit=*/ 0, MergeSorter::Mode::MergeUniqueChunks);
    state.emplace<ConnectingTail>(std::move(source));
}

void ExternalDistinctTransform::consumeMerged(Merging & merging)
{
    auto chunk = std::move(merging.chunk);
    if (!chunk.hasRows())
        return;

    /// Arrival numbers have served their purpose after the optional order-restoration sort.
    chassert(!output_chunk);
    output_chunk = spill_layout.restoreOutputChunk(std::move(chunk));
    result_rows += output_chunk.getNumRows();

    /// The row limit applies to the result. The hash set has been released, so no set memory remains
    /// to check against the byte limit. The row limit is checked before applying the hint.
    if (!set_size_limits.check(result_rows, /*bytes=*/ 0, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED)
        || (limit_hint && result_rows >= limit_hint))
        state.emplace<Finishing>();
}

ExternalDistinctTransform::PreparedMerge ExternalDistinctTransform::prepareMerge()
{
    const auto & spill_header = spill_layout.getSpillHeader();
    const auto & merged_header = spill_layout.getMergedHeader();

    /// The merger cannot consume its inputs until the final in-memory tail has been registered.
    PreparedMerge prepared;
    prepared.merger = std::make_shared<DistinctSortedTransform>(
        spill_header, merged_header, /*num_inputs=*/ 0, spill_layout.getRunSortDescription(),
        spill_layout.getFlagColumnPosition(), max_block_size_rows, /*have_all_inputs=*/ false);

    if (spill_layout.preservesInputOrder())
    {
        const auto & arrival_number_description = spill_layout.getArrivalNumberSortDescription();

        /// Restore arrival order after deduplication, spilling under the same memory policy as the runs.
        /// These rows are distinct, so the limit hint can bound the sort that restores their order.
        prepared.order_restoration.emplace_back(
            std::make_shared<PartialSortingTransform>(merged_header, arrival_number_description, limit_hint));
        prepared.order_restoration.emplace_back(std::make_shared<MergeSortingTransform>(
            merged_header,
            arrival_number_description,
            max_block_size_rows,
            /*max_block_bytes=*/ 0,
            limit_hint,
            /*increase_sort_description_compile_attempts=*/ false,
            /*max_bytes_before_remerge_=*/ 0,
            /*remerge_lowered_memory_bytes_ratio_=*/ 0.,
            minBytesInRun(),
            max_bytes_before_external_distinct,
            tmp_data,
            min_free_disk_space));
    }

    return prepared;
}

void ExternalDistinctTransform::connectMerge(PreparedMerge & prepared, Processors & processors)
{
    chassert(!merge_registration);
    auto * output = &prepared.merger->getOutputs().front();
    processors.emplace_back(prepared.merger);
    for (const auto & processor : prepared.order_restoration)
    {
        connect(*output, processor->getInputs().front());
        output = &processor->getOutputs().front();
        processors.emplace_back(processor);
    }

    inputs.emplace_back(*spill_layout.getMergedHeader(), this);
    connect(*output, inputs.back());
    merge_registration.emplace(std::move(prepared.merger), inputs.back());
}

OutputPort & ExternalDistinctTransform::connectRun(PreparedRun & prepared, Processors & processors)
{
    if (prepared.initial_merge)
        connectMerge(*prepared.initial_merge, processors);

    chassert(merge_registration);
    auto & merger = *merge_registration->merger;
    merger.addInput(*spill_layout.getSpillHeader());
    connect(prepared.source->getPort(), merger.getInputs().back());
    outputs.emplace_back(*spill_layout.getSpillHeader(), this);
    connect(outputs.back(), prepared.sink->getPort());
    processors.emplace_back(prepared.source);
    processors.emplace_back(prepared.sink);
    return outputs.back();
}

IProcessor::PipelineUpdate ExternalDistinctTransform::updatePipeline()
{
    Processors processors;
    std::visit([this, &processors]<typename Phase>(Phase & phase)
    {
        if constexpr (std::is_same_v<Phase, ConnectingSuppressionRun>)
        {
            auto run = std::move(phase.run);
            auto keys = std::move(phase.keys);
            auto & output = connectRun(run, processors);

            /// Suppression extraction waits for file finalization before preparing another run.
            /// The reader's completion dependency is relayed only after that wait finishes.
            inputs.emplace_back(Block(), this);
            auto & completion = inputs.back();
            connect(run.sink->getCompletionPort(), completion);
            outputs.emplace_back(Block(), this);
            auto & readiness = outputs.back();
            connect(readiness, run.source->getCompletionPort());
            state.emplace<WritingSuppressionRun>(std::move(run.progress), output, std::move(keys), completion, readiness);
        }
        else if constexpr (std::is_same_v<Phase, ConnectingInputRun>)
        {
            auto run = std::move(phase.run);
            auto & output = connectRun(run, processors);
            /// Ordinary input resumes after handoff; the reader waits directly for file completion.
            connect(run.sink->getCompletionPort(), run.source->getCompletionPort());
            state.emplace<WritingInputRun>(std::move(run.progress), output);
        }
        else if constexpr (std::is_same_v<Phase, ConnectingTail>)
        {
            chassert(merge_registration);
            auto source = std::move(phase.source);
            auto & merger = *merge_registration->merger;
            merger.addInput(*spill_layout.getSpillHeader());
            connect(source->getPort(), merger.getInputs().back());
            merger.setHaveAllInputs();
            auto & input = merge_registration->input;
            merge_registration.reset();
            processors.emplace_back(std::move(source));
            state.emplace<Merging>(input, Chunk{});
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "External DISTINCT has no pipeline update in state {}", state.index());
    }, state);

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

}
