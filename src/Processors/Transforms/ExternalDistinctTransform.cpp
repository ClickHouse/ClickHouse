#include <Processors/Transforms/ExternalDistinctTransform.h>

#include <algorithm>
#include <type_traits>

#include <Interpreters/sortBlock.h>
#include <Processors/Merges/DistinctSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
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
    bool preserve_input_order_,
    size_t max_external_merge_fan_in_)
    : IProcessor({header_}, {header_})
    , state(std::in_place_type<Hashing>, *header_, columns_, set_size_limits_)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , max_bytes_before_external_distinct(max_bytes_before_external_distinct_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_size_rows(max_block_size_rows_)
    , preserve_input_order(preserve_input_order_)
    , max_external_merge_fan_in(max_external_merge_fan_in_)
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
            || std::is_same_v<Phase, ConnectingInputRun> || std::is_same_v<Phase, ConnectingMerge>)
            return Status::UpdatePipeline;
        else if constexpr (std::is_same_v<Phase, WritingSuppressionRun> || std::is_same_v<Phase, WritingInputRun>)
            return prepareRunWrite(phase.progress, phase.output, phase.completion);
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

IProcessor::Status ExternalDistinctTransform::prepareRunWrite(
    RunWriteProgress & progress, OutputPort & output, InputPort & completion)
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
    if (completion.hasData())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected data on the external DISTINCT run completion port");

    if (!completion.isFinished())
    {
        completion.setNeeded();
        return Status::NeedData;
    }

    return Status::UpdatePipeline;
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

    hashing.set.prepareForInsert(input_chunk);

    /// Filtering can copy the normalized input before spilling, so allow another input-sized allocation
    /// and its row masks. Spill input needs fingerprints for generic keys and arrival numbers when
    /// preserving input order.
    /// A suppression run needs its columns, a sorted copy, and a permutation. Writing needs uncompressed,
    /// compressed, and file buffers. Oversized values and codec overhead can exceed this estimate.
    const size_t fingerprint_bytes = hashing.set.getKeyRepresentation() == DistinctKeyRepresentation::Hash128
        ? input_chunk.getNumRows() * sizeof(UInt128) : 0;
    const size_t arrival_numbers_bytes = preserve_input_order ? input_chunk.getNumRows() * sizeof(UInt64) : 0;
    const size_t suppression_columns_bytes = 2 * DEFAULT_BYTES_IN_RUN;
    const size_t sort_permutation_bytes = max_block_size_rows * sizeof(IColumn::Permutation::value_type);
    const size_t write_buffers_bytes = 3 * tmp_data->getSettings().buffer_size;
    const size_t spill_headroom_bytes
        = hashing.set.estimateFilteringMemory(input_chunk) + fingerprint_bytes + arrival_numbers_bytes
            + suppression_columns_bytes + sort_permutation_bytes + write_buffers_bytes;

    /// The threshold applies to total query memory, so current usage reduces the budget for growth.
    /// Query accounting can briefly become negative while a concurrent free saturates its counter.
    const UInt64 query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
    const UInt64 available_memory
        = max_bytes_before_external_distinct - std::min<UInt64>(max_bytes_before_external_distinct, query_memory_usage);

    const size_t growth_memory = hashing.set.estimateGrowthMemory(input_chunk);
    if (spill_headroom_bytes > available_memory || growth_memory > available_memory - spill_headroom_bytes)
    {
        LOG_TRACE(log, "Switching DISTINCT to external mode: {} "
            "(query memory: {}, spill threshold: {}, "
            "estimated peak extra memory for growth: {}, filtering and spill workspace: {})",
            query_memory_usage > max_bytes_before_external_distinct
                ? "query memory exceeded the spill threshold"
                : "projected allocations exceed the remaining spill-threshold budget",
            formatReadableSizeWithBinarySuffix(query_memory_usage),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct),
            formatReadableSizeWithBinarySuffix(growth_memory),
            formatReadableSizeWithBinarySuffix(spill_headroom_bytes));

        startSpilling(hashing);
        return;
    }

    consumed_rows += input_chunk.getNumRows();
    chassert(!output_chunk);
    output_chunk = hashing.set.filter(std::move(input_chunk));
    result_rows += output_chunk.getNumRows();

    /// A hint or a size limit in the `break` overflow mode retains this final result chunk.
    if ((limit_hint && result_rows >= limit_hint) || hashing.set.isLimitReached())
    {
        state.emplace<Finishing>();
        return;
    }

    /// Actual allocations and concurrent operators can consume more than the pre-insertion estimate.
    const Int64 query_memory_usage_after_insert = getCurrentQueryMemoryUsage();
    if (query_memory_usage_after_insert > static_cast<Int64>(max_bytes_before_external_distinct))
    {
        LOG_TRACE(log, "Switching DISTINCT to external mode: query memory exceeded the spill threshold after insertion "
            "(query memory: {}, spill threshold: {})",
            formatReadableSizeWithBinarySuffix(query_memory_usage_after_insert),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));
        startSpilling(hashing);
    }
}

void ExternalDistinctTransform::startSpilling(Hashing & hashing)
{
    chassert(!spill_layout);
    spill_layout.emplace(inputs.front().getSharedHeader(), hashing.set.getKeyColumnsPositions(),
        hashing.set.getKeyRepresentation(), preserve_input_order);

    if (hashing.set.getTotalRowCount())
    {
        LOG_TRACE(log, "Extracting {} DISTINCT suppression keys (set memory: {}) into sorted runs",
            hashing.set.getTotalRowCount(), formatReadableSizeWithBinarySuffix(hashing.set.getTotalByteCount()));
        auto keys = std::move(hashing.set).extractKeys();
        auto & extracting = state.emplace<ExtractingSuppression>(std::move(keys));
        extractSuppressionRun(extracting);
    }
    else
    {
        state.emplace<CollectingInput>();
        LOG_TRACE(log, "DISTINCT hash set is empty; collecting input for ordinary spill runs");
    }
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

        auto chunk = spill_layout->prepareSuppressionChunk(std::move(key_columns));
        Block block = spill_layout->getSuppressionRunHeader()->cloneWithColumns(chunk.detachColumns());

        /// Stable sorting preserves binary representatives of sort-equivalent keys. The emitted flag
        /// and optional arrival number are constant within this chunk, so key order satisfies run order.
        sortBlock(block, spill_layout->getKeySortDescription(), /*limit=*/ 0, IColumn::PermutationSortStability::Stable);
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
        LOG_TRACE(log, "Finished writing {} DISTINCT suppression runs; hash set released, collecting ordinary input "
            "(query memory: {})", temporary_files_num, formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()));
        return;
    }

    auto run = prepareRun(spill_layout->getSuppressionRunHeader(), std::move(chunks), bytes,
        spill_layout->getRunSortDescription(), MergeSorter::Mode::PreserveRows);
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
    auto prepared = spill_layout->prepareInputChunk(std::move(chunk), first_arrival_number);
    Block block = spill_layout->getInputRunHeader()->cloneWithColumns(prepared.detachColumns());

    /// Stable compaction keeps the first payload and permutes the service columns with its row.
    sortBlockAndDeduplicate(block, spill_layout->getKeySortDescription(), IColumn::PermutationSortStability::Stable);
    const auto rows = block.rows();
    Chunk sorted(block.detachColumns(), rows);
    collecting.bytes += sorted.allocatedBytes();
    collecting.chunks.push_back(std::move(sorted));

    /// An empty hash set produces no suppression files, so the first ordinary chunk starts a run.
    /// Later runs have a size floor when other operators keep query memory above the threshold.
    if (temporary_files_num == 0 || (collecting.bytes >= minBytesInRun()
        && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct)))
    {
        auto run = prepareRun(spill_layout->getInputRunHeader(), std::move(collecting.chunks), collecting.bytes,
            spill_layout->getKeySortDescription(), MergeSorter::Mode::MergeUniqueChunks);
        auto & connecting = state.emplace<ConnectingInputRun>(std::move(run));
        readRun(connecting.run.progress);
    }
}

ExternalDistinctTransform::PreparedRun ExternalDistinctTransform::prepareRun(
    SharedHeader run_header, Chunks chunks, size_t bytes, const SortDescription & description, MergeSorter::Mode mode)
{
    ++temporary_files_num;

    LOG_TRACE(log, "Will dump DISTINCT {} run #{} to disk "
        "(chunks: {}, buffered memory: {}, query memory: {}, spill threshold: {})",
        mode == MergeSorter::Mode::PreserveRows ? "suppression" : "ordinary",
        temporary_files_num, chunks.size(),
        formatReadableSizeWithBinarySuffix(bytes),
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    /// Reserving the run's space also preserves the configured amount of free disk space.
    TemporaryBlockStreamHolder tmp_stream(run_header, tmp_data, bytes + min_free_disk_space);

    /// The final merge applies the hint after suppression, which can remove keys from ordinary runs.
    auto merger = std::make_unique<MergeSorter>(
        run_header, std::move(chunks), description, max_block_size_rows, /*limit=*/ 0, mode);
    auto sink = std::make_shared<BufferingToFileSink>(run_header, std::move(tmp_stream), log);
    PreparedRun run{
        .progress = {std::move(merger), {}},
        .sink = std::move(sink),
    };
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
    LOG_TRACE(log, "Preparing final DISTINCT merge "
        "(temporary runs: {}, in-memory chunks: {}, restore input order: {})",
        temporary_files_num, tail.chunks.size(), spill_layout->preservesInputOrder());

    auto pipe = createMergePipe(std::move(tail.chunks));
    state.emplace<ConnectingMerge>(std::move(pipe));
}

void ExternalDistinctTransform::consumeMerged(Merging & merging)
{
    auto chunk = std::move(merging.chunk);
    if (!chunk.hasRows())
        return;

    /// Arrival numbers have served their purpose after the optional order-restoration sort.
    chassert(!output_chunk);
    output_chunk = spill_layout->restoreOutputChunk(std::move(chunk));
    result_rows += output_chunk.getNumRows();

    /// The row limit applies to the result. The hash set has been released, so no set memory remains
    /// to check against the byte limit. The row limit is checked before applying the hint.
    if (!set_size_limits.check(result_rows, /*bytes=*/ 0, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED)
        || (limit_hint && result_rows >= limit_hint))
        state.emplace<Finishing>();
}

Pipe ExternalDistinctTransform::createMergePipe(Chunks tail)
{
    const auto & merged_header = spill_layout->getMergedHeader();

    auto ordinary_header = spill_layout->getInputRunHeader();
    SourcePtr tail_source;
    if (!tail.empty())
        tail_source = std::make_shared<MergeSorterSource>(
            ordinary_header, std::move(tail), spill_layout->getKeySortDescription(),
            max_block_size_rows, /*limit=*/ 0, MergeSorter::Mode::MergeUniqueChunks);

    const auto description = spill_layout->getRunSortDescription();
    const auto num_key_columns = spill_layout->getKeySortDescription().size();
    const auto block_size = max_block_size_rows;
    auto suppression_merge = [header = spill_layout->getSuppressionRunHeader(), description, block_size]
        (const SharedHeaders & headers) -> ProcessorPtr
    {
        return std::make_shared<MergingSortedTransform>(
            header, headers.size(), description, block_size, /*max_block_size_bytes=*/ 0,
            /*max_dynamic_subcolumns=*/ std::nullopt, SortingQueueStrategy::Batch);
    };
    auto ordinary_merge = [ordinary_header, description, num_key_columns, block_size](const SharedHeaders & headers) -> ProcessorPtr
    {

        /// Ordinary intermediate chunks must be unique on the comparison keys. Retain fingerprints,
        /// the emitted flag, and arrival numbers when present so later passes can compare their rows.
        /// Keys already emitted before spilling are suppressed when both groups enter the final merge.
        return std::make_shared<DistinctSortedTransform>(headers, ordinary_header, description, num_key_columns, block_size);
    };
    auto final_merge = [merged_header, description, num_key_columns, block_size](const SharedHeaders & headers) -> ProcessorPtr
    {
        return std::make_shared<DistinctSortedTransform>(headers, merged_header, description, num_key_columns, block_size);
    };
    std::vector<ExternalMergeSource::Group> groups;
    groups.emplace_back(std::move(suppression_runs), std::move(suppression_merge));
    groups.emplace_back(std::move(ordinary_runs), std::move(ordinary_merge));
    Pipe pipe(std::make_shared<ExternalMergeSource>(
        merged_header, std::move(groups), std::move(tail_source), std::move(final_merge), max_external_merge_fan_in,
        tmp_data, min_free_disk_space, log));

    if (spill_layout->preservesInputOrder())
    {
        const auto & arrival_number_description = spill_layout->getArrivalNumberSortDescription();

        /// Restore arrival order after deduplication, spilling under the same memory policy as the runs.
        /// These rows are distinct, so the limit hint can bound the sort that restores their order.
        pipe.addTransform(
            std::make_shared<PartialSortingTransform>(merged_header, arrival_number_description, limit_hint));
        pipe.addTransform(std::make_shared<MergeSortingTransform>(
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
            min_free_disk_space,
            max_external_merge_fan_in));
    }

    return pipe;
}

void ExternalDistinctTransform::connectRun(PreparedRun & prepared, Processors & processors)
{
    outputs.emplace_back(prepared.sink->getPort().getHeader(), this);
    connect(outputs.back(), prepared.sink->getPort());
    inputs.emplace_back(Block(), this);
    connect(prepared.sink->getCompletionPort(), inputs.back());
    processors.emplace_back(prepared.sink);
}

IProcessor::PipelineUpdate ExternalDistinctTransform::updatePipeline()
{
    Processors processors;
    Processors finished;
    std::visit([this, &processors, &finished]<typename Phase>(Phase & phase)
    {
        if constexpr (std::is_same_v<Phase, ConnectingSuppressionRun>)
        {
            auto run = std::move(phase.run);
            auto keys = std::move(phase.keys);
            connectRun(run, processors);
            state.emplace<WritingSuppressionRun>(
                std::move(run.progress), outputs.back(), std::move(keys), inputs.back(), std::move(run.sink));
        }
        else if constexpr (std::is_same_v<Phase, ConnectingInputRun>)
        {
            auto run = std::move(phase.run);
            connectRun(run, processors);
            state.emplace<WritingInputRun>(std::move(run.progress), outputs.back(), inputs.back(), std::move(run.sink));
        }
        else if constexpr (std::is_same_v<Phase, WritingSuppressionRun> || std::is_same_v<Phase, WritingInputRun>)
        {
            auto & runs = std::is_same_v<Phase, WritingSuppressionRun> ? suppression_runs : ordinary_runs;
            runs.emplace_back(phase.sink->releaseFile());
            disconnect(outputs.back(), phase.sink->getPort());
            disconnect(phase.sink->getCompletionPort(), inputs.back());
            outputs.pop_back();
            inputs.pop_back();
            finished.emplace_back(std::move(phase.sink));
            if constexpr (std::is_same_v<Phase, WritingSuppressionRun>)
            {
                auto keys = std::move(phase.keys);
                state.emplace<ExtractingSuppression>(std::move(keys));
            }
            else
                state.emplace<CollectingInput>();
        }
        else if constexpr (std::is_same_v<Phase, ConnectingMerge>)
        {
            inputs.emplace_back(phase.pipe.getHeader(), this);
            connect(*phase.pipe.getOutputPort(0), inputs.back());
            processors = Pipe::detachProcessors(std::move(phase.pipe));
            state.emplace<Merging>(inputs.back(), Chunk{});
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "External DISTINCT has no pipeline update in state {}", state.index());
    }, state);

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = std::move(finished)};
}

}
