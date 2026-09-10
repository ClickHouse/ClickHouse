#include <Processors/Transforms/ExternalDistinctTransform.h>

#include <algorithm>

#include <Interpreters/sortBlock.h>
#include <Processors/Merges/DistinctSortedTransform.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/SortingTransform.h>
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
    , distinct_set(
          std::in_place, *header_, columns_, set_size_limits_, /*skip_null_keys_=*/ false, /*require_extractable_keys_=*/ true)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , max_bytes_before_external_distinct(max_bytes_before_external_distinct_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_size_rows(max_block_size_rows_)
    , spill_layout(header_, distinct_set->getKeyColumnsPositions(), preserve_input_order_)
{
}

ExternalDistinctTransform::~ExternalDistinctTransform() = default;

size_t ExternalDistinctTransform::minBytesInRun() const
{
    return std::min(max_bytes_before_external_distinct, DEFAULT_BYTES_IN_RUN);
}

Chunk ExternalDistinctTransform::sortSpillChunk(Chunk chunk, RunKind kind) const
{
    /// Stable sorting retains the first-arriving payload and the first binary representation among
    /// keys that compare equal. The flag is constant within a chunk, so key order also satisfies the
    /// run order. The service columns follow the same permutation as the input columns.
    Block block = spill_layout.getSpillHeader()->cloneWithColumns(chunk.detachColumns());
    if (kind == RunKind::Suppression)
        sortBlock(block, spill_layout.getKeySortDescription(), /*limit=*/ 0, IColumn::PermutationSortStability::Stable);
    else
        sortBlockAndDeduplicate(block, spill_layout.getKeySortDescription(), IColumn::PermutationSortStability::Stable);

    return Chunk(block.getColumns(), block.rows());
}

void ExternalDistinctTransform::startFirstSpill()
{
    LOG_TRACE(log, "Switching DISTINCT to the external mode (query memory: {}, spill threshold: {})",
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    if (distinct_set->getTotalRowCount())
        suppression_keys = std::move(*distinct_set).extractKeys();
    distinct_set.reset();
    stage = suppression_keys ? Stage::ExtractSuppression : Stage::Consume;
}

void ExternalDistinctTransform::extractSuppressionRun()
{
    Chunks run_chunks;
    size_t run_bytes = 0;

    /// Bound the working columns to one run while the extractor retains the set and arena. A complete
    /// key can exceed the byte target, and sorting needs additional temporary buffers.
    while (!isCancelled() && run_bytes < DEFAULT_BYTES_IN_RUN)
    {
        auto key_columns = suppression_keys->next(max_block_size_rows, DEFAULT_BYTES_IN_RUN - run_bytes);
        if (key_columns.empty())
        {
            suppression_keys.reset();
            break;
        }

        auto prepared = sortSpillChunk(
            spill_layout.prepareSuppressionChunk(std::move(key_columns)), RunKind::Suppression);
        run_bytes += prepared.allocatedBytes();
        run_chunks.push_back(std::move(prepared));
    }

    if (isCancelled())
        return;

    if (run_chunks.empty())
        stage = Stage::Consume;
    else
    {
        startSpillRun(std::move(run_chunks), run_bytes, RunKind::Suppression);
        FailPointInjection::pauseFailPoint(FailPoints::external_distinct_suppression_run_prepared_pause);
    }
}

void ExternalDistinctTransform::startSpillRun(Chunks run_chunks, size_t run_bytes, RunKind kind)
{
    const auto & spill_header = spill_layout.getSpillHeader();
    ++temporary_files_num;

    LOG_TRACE(log, "Will dump distinct run ({} chunks, {}) to disk (query memory: {}, limit: {})",
        run_chunks.size(),
        formatReadableSizeWithBinarySuffix(run_bytes),
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    /// Reserving the run's space also preserves the configured amount of free disk space.
    const size_t reserve_size = run_bytes + min_free_disk_space;
    TemporaryBlockStreamHolder tmp_stream(spill_header, tmp_data, reserve_size);

    const auto mode = kind == RunKind::Input ? MergeSorter::Mode::MergeUniqueChunks : MergeSorter::Mode::PreserveRows;
    const auto & description = kind == RunKind::Input
        ? spill_layout.getKeySortDescription() : spill_layout.getRunSortDescription();
    /// The final merge applies the hint after suppression, which can remove keys from ordinary runs.
    merge_sorter = std::make_unique<MergeSorter>(
        spill_header, std::move(run_chunks), description, max_block_size_rows, /*limit=*/ 0, mode);

    auto sink = std::make_shared<BufferingToFileSink>(spill_header, std::move(tmp_stream), log);
    auto source = std::make_shared<BufferingFromFileSource>(spill_header, sink->getHolder(), log);
    PendingPipelineUpdate update{
        .kind = distinct_merger ? PipelineUpdateKind::AddRun : PipelineUpdateKind::InitializeMergeAndAddRun,
        .run_kind = kind,
        .sink = sink,
        .source = source,
        .merged_stream = {},
        .processors = {source, sink},
    };

    if (update.kind == PipelineUpdateKind::InitializeMergeAndAddRun)
        createMergedStream(update);

    pending_pipeline_update = std::move(update);
    stage = Stage::Serialize;
    sum_bytes_in_chunks = 0;
}

void ExternalDistinctTransform::createMergedStream(PendingPipelineUpdate & update)
{
    const auto & spill_header = spill_layout.getSpillHeader();
    const auto & merged_header = spill_layout.getMergedHeader();

    /// The merger cannot consume its inputs until the final in-memory tail has been registered.
    distinct_merger = std::make_shared<DistinctSortedTransform>(
        spill_header, merged_header, /*num_inputs=*/ 0, spill_layout.getRunSortDescription(),
        spill_layout.getFlagColumnPosition(), max_block_size_rows, /*have_all_inputs=*/ false);
    update.processors.emplace_back(distinct_merger);

    if (spill_layout.preservesInputOrder())
    {
        const auto & arrival_number_description = spill_layout.getArrivalNumberSortDescription();

        /// Restore arrival order after deduplication, spilling under the same memory policy as the runs.
        /// These rows are distinct, so the limit hint can bound the sort that restores their order.
        update.merged_stream.emplace_back(
            std::make_shared<PartialSortingTransform>(merged_header, arrival_number_description, limit_hint));
        update.merged_stream.emplace_back(std::make_shared<MergeSortingTransform>(
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

    for (const auto & processor : update.merged_stream)
        update.processors.emplace_back(processor);
}

void ExternalDistinctTransform::connectMergedStream(const Processors & merged_stream)
{
    auto * output = &distinct_merger->getOutputs().front();
    for (const auto & processor : merged_stream)
    {
        connect(*output, processor->getInputs().front());
        output = &processor->getOutputs().front();
    }

    inputs.emplace_back(*spill_layout.getMergedHeader(), this);
    merged_input = &inputs.back();
    connect(*output, *merged_input);
}

void ExternalDistinctTransform::attachSpilledRun(const ProcessorPtr & source, const ProcessorPtr & sink, RunKind kind)
{
    distinct_merger->addInput(*spill_layout.getSpillHeader());
    connect(source->getOutputs().back(), distinct_merger->getInputs().back());

    outputs.emplace_back(*spill_layout.getSpillHeader(), this);
    run_write_output = &outputs.back();
    connect(*run_write_output, sink->getInputs().back());

    if (kind == RunKind::Input)
    {
        run_completion_input = nullptr;
        run_readiness_output = nullptr;
        /// Ordinary input can continue while its run finishes writing; the source waits for the sink.
        connect(sink->getOutputs().front(), source->getInputs().front());
        return;
    }

    /// Suppression extraction waits for the sink to finalize the file and release its writing buffers
    /// before preparing another run. The source remains gated until the file is complete.
    inputs.emplace_back(Block(), this);
    run_completion_input = &inputs.back();
    connect(sink->getOutputs().front(), *run_completion_input);

    outputs.emplace_back(Block(), this);
    run_readiness_output = &outputs.back();
    connect(*run_readiness_output, source->getInputs().front());
}

void ExternalDistinctTransform::attachInMemoryTail(const ProcessorPtr & source)
{
    distinct_merger->addInput(*spill_layout.getSpillHeader());
    connect(source->getOutputs().back(), distinct_merger->getInputs().back());
    distinct_merger->setHaveAllInputs();
    merge_inputs_finalized = true;
}

IProcessor::PipelineUpdate ExternalDistinctTransform::updatePipeline()
{
    auto update = std::move(*pending_pipeline_update);
    pending_pipeline_update.reset();

    switch (update.kind)
    {
        case PipelineUpdateKind::InitializeMergeAndAddRun:
            connectMergedStream(update.merged_stream);
            [[fallthrough]];
        case PipelineUpdateKind::AddRun:
            attachSpilledRun(update.source, update.sink, update.run_kind);
            break;
        case PipelineUpdateKind::AddInMemoryTail:
            attachInMemoryTail(update.source);
            break;
    }

    return PipelineUpdate{.to_add = std::move(update.processors), .to_remove = {}};
}

IProcessor::Status ExternalDistinctTransform::prepare()
{
    if (stage == Stage::Serialize)
    {
        if (pending_pipeline_update)
            return Status::UpdatePipeline;

        auto status = prepareSerialize();
        if (status != Status::Finished)
            return status;

        stage = suppression_keys ? Stage::ExtractSuppression : Stage::Consume;
    }

    if (stage == Stage::ExtractSuppression)
        return Status::Ready;

    if (stage == Stage::Consume)
    {
        auto status = prepareConsume();
        if (status != Status::Finished)
            return status;

        stage = Stage::Generate;
    }

    if (pending_pipeline_update)
        return Status::UpdatePipeline;

    if (distinct_merger && !merge_inputs_finalized)
        return Status::Ready;

    return prepareGenerate();
}

IProcessor::Status ExternalDistinctTransform::prepareConsume()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// A pending chunk from the hashing phase precedes any output from the merged runs.
    if (generated_chunk)
        output.push(std::move(generated_chunk));

    if (read_stopped)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    if (pending_input)
        current_chunk = std::move(pending_input);

    if (!current_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        current_chunk = input.pull(true);
    }

    return Status::Ready;
}

IProcessor::Status ExternalDistinctTransform::prepareSerialize()
{
    if (!run_write_output->isFinished())
    {
        if (!run_write_output->canPush())
            return Status::PortFull;

        if (current_chunk)
            run_write_output->push(std::move(current_chunk));

        if (merge_sorter)
            return Status::Ready;

        run_write_output->finish();
    }

    if (run_completion_input)
    {
        if (!run_completion_input->isFinished())
        {
            run_completion_input->setNeeded();
            return Status::NeedData;
        }

        run_readiness_output->finish();
    }

    return Status::Finished;
}

IProcessor::Status ExternalDistinctTransform::prepareGenerate()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    /// Without an external merger, all distinct rows were already emitted while consuming input.
    if (!distinct_merger)
    {
        output.finish();
        return Status::Finished;
    }

    if (read_stopped)
    {
        for (auto & input : inputs)
            input.close();

        output.finish();
        return Status::Finished;
    }

    auto & input = *merged_input;

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    /// Restoring the output representation belongs to `work`.
    return Status::Ready;
}

void ExternalDistinctTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));

    if (stage == Stage::ExtractSuppression)
        extractSuppressionRun();

    if (stage == Stage::Serialize)
        serialize();

    if (stage == Stage::Generate)
        generate();
}

void ExternalDistinctTransform::consume(Chunk chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (distinct_set)
    {
        /// Filtering can copy the input before spilling, so allow another input-sized allocation.
        /// A suppression run needs its columns, a sorted copy, and a permutation. Writing needs the
        /// uncompressed, compressed, and file buffers. Oversized values and codec overhead can exceed
        /// this estimate.
        const size_t suppression_columns_bytes = 2 * DEFAULT_BYTES_IN_RUN;
        const size_t sort_permutation_bytes = max_block_size_rows * sizeof(IColumn::Permutation::value_type);
        const size_t write_buffers_bytes = 3 * tmp_data->getSettings().buffer_size;
        const size_t spill_headroom_bytes
            = chunk.allocatedBytes() + suppression_columns_bytes + sort_permutation_bytes + write_buffers_bytes;
        distinct_set->prepareForInsert(chunk);
        if (const auto available = getMostStrictAvailableSystemMemory())
        {
            const size_t growth_memory = distinct_set->estimateGrowthMemory(chunk.getNumRows());
            if (growth_memory && (spill_headroom_bytes > *available || growth_memory > *available - spill_headroom_bytes))
            {
                pending_input = std::move(chunk);
                startFirstSpill();
                return;
            }
        }
    }

    const UInt64 first_arrival_number = consumed_rows;
    consumed_rows += chunk.getNumRows();

    if (distinct_set)
    {
        Chunk filtered = distinct_set->filter(std::move(chunk));
        if (filtered.hasRows())
        {
            emitted_rows += filtered.getNumRows();
            generated_chunk = std::move(filtered);

            if (limit_hint && emitted_rows >= limit_hint)
            {
                read_stopped = true;
                return;
            }
        }

        /// A size limit with the 'break' overflow mode was reached: the partial chunk above is still
        /// emitted, and no further input can produce output.
        if (distinct_set->isLimitReached())
        {
            read_stopped = true;
            return;
        }

        if (getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct))
            startFirstSpill();
    }
    else
    {
        auto prepared = sortSpillChunk(
            spill_layout.prepareInputChunk(std::move(chunk), first_arrival_number), RunKind::Input);
        sum_bytes_in_chunks += prepared.allocatedBytes();
        chunks.push_back(std::move(prepared));

        /// The first input run initializes the merger when the set had no suppression keys. Later runs
        /// have a size floor to avoid one file per chunk when another operator keeps query memory above
        /// the spill threshold.
        if (!distinct_merger || (sum_bytes_in_chunks >= minBytesInRun()
            && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct)))
        {
            auto run_chunks = std::move(chunks);
            chunks.clear();
            startSpillRun(std::move(run_chunks), sum_bytes_in_chunks, RunKind::Input);
        }
    }
}

void ExternalDistinctTransform::serialize()
{
    /// Reads can consume only duplicates and return zero rows. Skip those chunks while retaining
    /// cancellation checks between reads, and finish writing only when the merger is exhausted.
    while (!isCancelled())
    {
        current_chunk = merge_sorter->read();
        if (!current_chunk)
            break;

        if (current_chunk.hasRows())
            return;
    }

    merge_sorter.reset();
}

void ExternalDistinctTransform::generate()
{
    if (!merge_inputs_finalized)
    {
        ProfileEvents::increment(ProfileEvents::ExternalDistinctMerge);
        LOG_INFO(log, "There are {} temporary distinct runs to merge", temporary_files_num);

        /// Register the final input even when the tail is empty, then close merge-input registration.
        /// The tail is merged into unique chunks under the same contract as ordinary disk runs.
        auto source = std::make_shared<MergeSorterSource>(
            spill_layout.getSpillHeader(), std::move(chunks), spill_layout.getKeySortDescription(),
            max_block_size_rows, /*limit=*/ 0, MergeSorter::Mode::MergeUniqueChunks);
        pending_pipeline_update.emplace(PendingPipelineUpdate{
            .kind = PipelineUpdateKind::AddInMemoryTail,
            .run_kind = RunKind::Input,
            .sink = {},
            .source = source,
            .merged_stream = {},
            .processors = {source},
        });
        return;
    }

    if (!current_chunk || !current_chunk.hasRows())
        return;

    /// The chunk is merged, deduplicated and, when the input order is preserved, sorted back by the
    /// arrival numbers, which have done their job by now.
    Chunk chunk = spill_layout.restoreOutputChunk(std::move(current_chunk));

    emitted_rows += chunk.getNumRows();
    generated_chunk = std::move(chunk);

    /// The rows limit applies to the emitted result. The hash set has been released, so there is no
    /// set memory left to check against the byte limit.
    if (!set_size_limits.check(emitted_rows, /*bytes=*/ 0, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED)
        || (limit_hint && emitted_rows >= limit_hint))
        read_stopped = true;
}

}
