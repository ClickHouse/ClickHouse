#include <Processors/Transforms/ExternalGroupLimitTransform.h>

#include <algorithm>
#include <type_traits>
#include <utility>

#include <Interpreters/Squashing.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Merges/GroupLimitSortedTransform.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/ExternalDistinctTransform.h>
#include <Processors/Transforms/ExternalLimitByTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char external_group_limit_seen_run_prepared_pause[];
    extern const char external_group_limit_skip_spill_floor[];
}

namespace
{

/// Seen-key extraction targets this many bytes per run, independently of the spill threshold.
/// Input runs use the smaller of this value and the threshold as their minimum accumulated size.
constexpr size_t DEFAULT_BYTES_IN_RUN = DEFAULT_BLOCK_SIZE * 256;

/// Sorting units combine small input chunks before applying the group limit. Their row and byte targets
/// are independent of the smaller blocks written to spill files. An oversized input is sorted alone.
constexpr size_t MAX_BYTES_IN_SORTING_UNIT = 16 << 20;

size_t estimateRunReadMemory(size_t max_block_bytes, size_t buffer_size)
{
    /// The merger can retain a block while the source reads its successor. Allow twice the observed
    /// allocation for each block because deserialization and column growth can use more capacity.
    /// Compression blocks follow the writer's buffer size. The file reader separately uses at most
    /// `DBMS_DEFAULT_BUFFER_SIZE`, including when the configured writer buffer is smaller.
    return 4 * max_block_bytes + 2 * buffer_size + DBMS_DEFAULT_BUFFER_SIZE;
}

}

template <typename Traits>
ExternalGroupLimitTransform<Traits>::ExternalGroupLimitTransform(
    SharedHeader header_,
    const GroupLimitParams & params_,
    size_t max_bytes_before_external_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_,
    size_t max_block_size_rows_,
    size_t preferred_block_bytes_,
    bool preserve_input_order_,
    DistinctSetMemoryTracker::SharedCounter shared_filter_bytes_,
    SortDescription input_sort_description_)
    : IProcessor({header_}, {header_})
    , params(params_)
    , filter_memory(shared_filter_bytes_)
    , state(std::in_place_type<Hashing>, *header_, params_, *this)
    , max_bytes_before_external(max_bytes_before_external_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_size_rows(max_block_size_rows_)
    , preferred_block_bytes(preferred_block_bytes_)
    , preserve_input_order(preserve_input_order_)
    , input_order_description(
          preserve_input_order_ && GroupLimitSpillLayout::spillsSortColumns(*header_, input_sort_description_)
              ? std::move(input_sort_description_)
              : SortDescription{})
{
    chassert(params.group_limit_end > 0);
    chassert(!shared_filter_bytes_ || !params.size_limits.hasLimits());
}

template <typename Traits>
ExternalGroupLimitTransform<Traits>::~ExternalGroupLimitTransform() = default;

template <typename Traits>
size_t ExternalGroupLimitTransform<Traits>::minBytesInRun() const
{
    return max_bytes_before_external ? std::min(max_bytes_before_external, DEFAULT_BYTES_IN_RUN) : DEFAULT_BYTES_IN_RUN;
}

template <typename Traits>
bool ExternalGroupLimitTransform<Traits>::isBelowSpillFloor(size_t state_bytes, size_t growth_bytes) const
{
    fiu_do_on(FailPoints::external_group_limit_skip_spill_floor, { return false; });
    const size_t floor = minBytesInRun() / params.num_streams;
    return growth_bytes < floor && state_bytes < floor - growth_bytes;
}

template <typename Traits>
const typename ExternalGroupLimitTransform<Traits>::CollectingInput * ExternalGroupLimitTransform<Traits>::getCollectingInput() const
{
    if (const auto * collecting = std::get_if<CollectingInput>(&state))
        return collecting;
    if (const auto * tail = std::get_if<PreparingTail>(&state))
        return &tail->collecting;
    if (const auto * tail = std::get_if<ConnectingTailRun>(&state))
        return &tail->remaining;
    if (const auto * tail = std::get_if<WritingTailRun>(&state))
        return &tail->remaining;
    return nullptr;
}

template <typename Traits>
typename ExternalGroupLimitTransform<Traits>::CollectingInput * ExternalGroupLimitTransform<Traits>::getCollectingInput()
{
    return const_cast<CollectingInput *>(std::as_const(*this).getCollectingInput());
}

template <typename Traits>
ProcessorMemoryStats ExternalGroupLimitTransform<Traits>::getMemoryStats() const
{
    ProcessorMemoryStats res;
    if (const auto * hashing = std::get_if<Hashing>(&state); hashing && !hashing->input_finished && hashing->filter.getTotalRowCount())
    {
        res.spillable_memory_bytes = hashing->filter.getTotalByteCount();
        /// Allow the bounded seen-group extraction/sorting workspace.
        res.need_reserved_memory_bytes = 2 * DEFAULT_BYTES_IN_RUN
            + estimateSortingWorkspace(maxRowsInSortingUnit()) + 3 * tmp_data->getSettings().buffer_size;
    }
    else if (const auto * collecting = getCollectingInput())
    {
        res.spillable_memory_bytes = collecting->sorted_bytes + collecting->pending.allocated_bytes;
        if (res.spillable_memory_bytes)
        {
            res.need_reserved_memory_bytes = 3 * collecting->pending.allocated_bytes
                + estimateSortingWorkspace(collecting->pending.rows)
                + estimateRunWriteMemory(collecting->sorted_rows + collecting->pending.rows, res.spillable_memory_bytes);
        }
    }

    return res;
}

template <typename Traits>
size_t ExternalGroupLimitTransform<Traits>::spill(size_t at_least_bytes)
{
    size_t bytes = getMemoryStats().spillable_memory_bytes;
    if (!at_least_bytes || !bytes || isCancelled())
        return 0;

    if (auto * hashing = std::get_if<Hashing>(&state))
    {
        LOG_TRACE(log, "Switching {} to external mode: scheduler requested spilling (requested bytes: {}, state memory: {}, query memory: {})",
            Traits::operator_name,
            formatReadableSizeWithBinarySuffix(at_least_bytes),
            formatReadableSizeWithBinarySuffix(bytes),
            formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()));
        startSpilling(*hashing);
        /// The extractor retains the grouping state until its last group is materialized. Drain every seen run
        /// before settling the scheduler's request, keeping only one run's working columns alive.
        while (auto * connecting = std::get_if<ConnectingSeenRun>(&state))
        {
            auto run = std::move(connecting->run);
            auto extractor = std::move(connecting->extractor);
            spillRun(std::move(run));
            auto & extracting = state.template emplace<ExtractingSeenGroups>(std::move(extractor));
            extractSeenRun(extracting);
        }
    }
    else
    {
        /// A prepared tail prefix must be registered before any later suffix, so equal keys keep their
        /// earliest rows. Finish its unconnected run before spilling more of the tail.
        if (auto * connecting = std::get_if<ConnectingTailRun>(&state))
        {
            spillRun(std::move(connecting->run));
            auto remaining = std::move(connecting->remaining);
            state.template emplace<PreparingTail>(std::move(remaining));
        }

        auto * collecting = getCollectingInput();
        chassert(collecting);
        flushSortingUnit(*collecting);
        /// Spill a prefix of whole chunks so earlier runs still win ties for the earliest rows.
        Chunks chunks;
        bytes = 0;
        size_t rows = 0;
        for (auto & chunk : collecting->sorted_chunks)
        {
            bytes += chunk.allocatedBytes();
            rows += chunk.getNumRows();
            chunks.emplace_back(std::move(chunk));
            if (bytes >= at_least_bytes)
                break;
        }
        collecting->sorted_chunks.erase(collecting->sorted_chunks.begin(), collecting->sorted_chunks.begin() + chunks.size());
        collecting->sorted_bytes -= bytes;
        collecting->sorted_rows -= rows;
        if (!std::holds_alternative<CollectingInput>(state))
            ProfileEvents::increment(Traits::tailSpilledRowsEvent(), rows);

        auto run = prepareRun(spill_layout->getInputRunHeader(), std::move(chunks), bytes,
            spill_layout->getKeySortDescription(), MergeSorter::Mode::LimitGroupRows);
        spillRun(std::move(run));
    }

    return isCancelled() ? 0 : bytes;
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::spillRun(PreparedRun run)
{
    /// None of these processors have been connected yet, so writing does not depend on pipeline work.
    while (run.progress.chunk || run.progress.merger)
    {
        if (run.progress.chunk)
            run.sink->consume(std::move(run.progress.chunk));
        if (run.progress.merger)
            readRun(run.progress);
    }
    run.sink->onFinish();
    run.source = std::make_shared<BufferingFromFileSource>(
        run.source->getPort().getSharedHeader(), std::move(run.sink->getHolder()), log);
    run.sink.reset();
    spilled_runs.emplace_back(std::move(run));
}

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepare()
{
    if (outputs.front().isFinished())
    {
        /// Closing every dependency lets active sinks and readers terminate. Their resources remain
        /// owned by the pipeline until destruction, outside the executor's preparation lock.
        return finish();
    }

    if (!spilled_runs.empty())
        return Status::UpdatePipeline;

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
        else if constexpr (std::is_same_v<Phase, ExtractingSeenGroups> || std::is_same_v<Phase, PreparingTail>)
            return Status::Ready;
        else if constexpr (std::is_same_v<Phase, ConnectingSeenRun>
            || std::is_same_v<Phase, ConnectingInputRun> || std::is_same_v<Phase, ConnectingTailRun>
            || std::is_same_v<Phase, ConnectingTail>)
            return Status::UpdatePipeline;
        else if constexpr (std::is_same_v<Phase, WritingSeenRun>)
            return prepareSeenRunWrite(phase);
        else if constexpr (std::is_same_v<Phase, WritingInputRun>)
            return prepareInputWrite(phase);
        else if constexpr (std::is_same_v<Phase, WritingTailRun>)
            return prepareTailWrite(phase);
        else if constexpr (std::is_same_v<Phase, Merging>)
            return prepareMergedOutput(phase);
        else if constexpr (std::is_same_v<Phase, Finishing>)
            return prepareFinish();
    }, state);
}

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareInput()
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

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareCollectingInput(CollectingInput & collecting)
{
    auto status = prepareInput();
    if (status == Status::Finished)
    {
        auto remaining = std::move(collecting);
        state.template emplace<PreparingTail>(std::move(remaining));
        return Status::Ready;
    }
    return status;
}

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareRunWrite(RunWriteProgress & progress, OutputPort & output)
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

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareSeenRunWrite(WritingSeenRun & writing)
{
    auto status = prepareRunWrite(writing.progress, writing.output);
    if (status != Status::Finished)
        return status;

    if (writing.completion.hasData())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected data on the external {} run completion port", Traits::operator_name);

    if (!writing.completion.isFinished())
    {
        writing.completion.setNeeded();
        return Status::NeedData;
    }

    writing.readiness.finish();
    auto extractor = std::move(writing.extractor);
    state.template emplace<ExtractingSeenGroups>(std::move(extractor));
    return Status::Ready;
}

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareInputWrite(WritingInputRun & writing)
{
    auto status = prepareRunWrite(writing.progress, writing.output);
    if (status != Status::Finished)
        return status;

    auto & collecting = state.template emplace<CollectingInput>();
    /// Producer completion is sufficient here; the file reader waits for the sink independently.
    return prepareCollectingInput(collecting);
}

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareTailWrite(WritingTailRun & writing)
{
    auto status = prepareRunWrite(writing.progress, writing.output);
    if (status != Status::Finished)
        return status;

    chassert(!writing.completion.hasData());
    if (!writing.completion.isFinished())
    {
        writing.completion.setNeeded();
        return Status::NeedData;
    }

    /// The file must finish before budgeting the remaining tail, so its writer has released the
    /// compression buffers and the prefix's final output block. Readers stay idle until final input
    /// registration closes.
    writing.readiness.finish();
    auto remaining = std::move(writing.remaining);
    state.template emplace<PreparingTail>(std::move(remaining));
    return Status::Ready;
}

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareMergedOutput(Merging & merging)
{
    auto & output = outputs.front();
    if (!output.canPush())
        return Status::PortFull;

    if (output_chunk)
        output.push(std::move(output_chunk));

    if (merging.input.isFinished())
    {
        state.template emplace<Finishing>();
        return prepareFinish();
    }

    merging.input.setNeeded();
    if (!merging.input.hasData())
        return Status::NeedData;

    merging.chunk = merging.input.pull(true);
    return Status::Ready;
}

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::prepareFinish()
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

template <typename Traits>
IProcessor::Status ExternalGroupLimitTransform<Traits>::finish()
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

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::work()
{
    std::visit([this]<typename Phase>(Phase & phase)
    {
        if constexpr (std::is_same_v<Phase, Hashing>)
        {
            /// A completed filter keeps its bytes in the shared counter until the transform is destroyed. A
            /// single-stream `DISTINCT` would still retain these keys while other partitions insert theirs,
            /// so the combined byte limit must count them even if this partition finishes first.
            if (phase.input_finished)
                state.template emplace<Finishing>();
            else
                consumeHashing(phase);
        }
        else if constexpr (std::is_same_v<Phase, ExtractingSeenGroups>)
            extractSeenRun(phase);
        else if constexpr (std::is_same_v<Phase, CollectingInput>)
            collectInput(phase);
        else if constexpr (std::is_same_v<Phase, WritingSeenRun>
            || std::is_same_v<Phase, WritingInputRun> || std::is_same_v<Phase, WritingTailRun>)
            readRun(phase.progress);
        else if constexpr (std::is_same_v<Phase, PreparingTail>)
            prepareTail(phase);
        else if constexpr (std::is_same_v<Phase, Merging>)
            consumeMerged(phase);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "External {} has no work in state {}", Traits::operator_name, state.index());
    }, state);
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::addResultRows(size_t rows)
{
    result_rows += rows;
    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::consumeHashing(Hashing & hashing)
{
    if (unlikely(!input_chunk.hasRows()))
    {
        /// A rowless chunk with a `ChunkInfo` is a marker for the consumer (e.g. a watermark of a streaming
        /// read). Before spilling, the results of the preceding chunks are already emitted, so it keeps its place.
        chassert(!output_chunk);
        if (!input_chunk.getChunkInfos().empty())
            output_chunk = std::move(input_chunk);
        input_chunk.clear();
        return;
    }

    hashing.filter.prepareForInsert(input_chunk);
    filter_memory.update(hashing.getRetainedFilterBytes());

    /// The input columns and existing filter state are already charged to query memory. The estimates below
    /// cover additional allocations for inserting keys, filtering rows, and preparing the first spill run.
    ///
    /// Filtering can allocate masks marking rows to keep, packed keys combining multiple key columns
    /// into one value per row, and output copies. The masks and packed keys are released before spilling.
    const size_t filtering_memory = hashing.filter.estimateFilteringMemory(input_chunk);

    /// The prepared input is already charged to query memory. Its allocated size estimates the cost
    /// of another copy when filtering, sorting, or cutting the unprocessed part of a chunk.
    const size_t input_bytes = input_chunk.allocatedBytes();

    /// Service columns store internal spill metadata: fingerprints for keys compared by hash and
    /// arrival numbers recording each row's original position. These columns are added when the key
    /// representation needs fingerprints or the result must preserve input order.
    const size_t service_columns_bytes = GroupLimitSpillLayout::estimateServiceColumnsMemory(
        input_chunk.getNumRows(), hashing.filter.getKeyRepresentation(), attachesArrivalNumbers());

    /// A seen run contains keys already counted before spilling, allowing the final merge to rank
    /// later rows. Extraction targets `DEFAULT_BYTES_IN_RUN` bytes per run; twice that target covers
    /// extracted columns alongside their sorted copies. This is a soft target because one large key or a
    /// column allocation can exceed it.
    const size_t seen_columns_bytes = 2 * DEFAULT_BYTES_IN_RUN;

    /// Input runs contain rows still to be limited per group. Sorting can retain the input and service
    /// columns alongside their permuted copies. The input is already charged, so budget one input copy
    /// and both the original and copied service columns.
    const size_t input_columns_bytes = input_bytes + 2 * service_columns_bytes;

    /// Seen-key extraction prepares a sorting unit at a time; the first input run can contain the whole
    /// input chunk. Reserve sorting indices and their temporary arrays for the larger input.
    const size_t sort_rows = std::max<size_t>(maxRowsInSortingUnit(), input_chunk.getNumRows());
    const size_t sorting_workspace = estimateSortingWorkspace(sort_rows);

    /// Writing a temporary file can hold uncompressed input, compressed output, and a file buffer
    /// at the same time. The estimate allows three configured buffer sizes; oversized values and codec
    /// overhead can exceed it.
    const size_t write_buffers_bytes = 3 * tmp_data->getSettings().buffer_size;

    /// A filtered output copy can remain pending while seen groups are extracted. The original input can
    /// still be shared upstream, so this output copy needs an additional `input_bytes`. Seen-key
    /// extraction and input sorting run separately; take the larger column estimate, then add allowances
    /// for sorting workspace and buffers for writing the file.
    const size_t spill_memory
        = std::max(input_bytes + seen_columns_bytes, input_columns_bytes) + sorting_workspace + write_buffers_bytes;

    /// These values retain the last check's tracked query usage, additional memory for new keys, and
    /// temporary workspace estimate so the spill log describes the check that rejected insertion.
    UInt64 query_memory_usage = 0;
    size_t growth_memory = 0;
    size_t workspace_memory = 0;
    auto check_memory_budget = [&](size_t growth, size_t workspace)
    {
        growth_memory = growth;
        workspace_memory = workspace;
        /// The spill threshold applies to total query memory, so current usage reduces the budget for
        /// additional allocations.
        /// Query accounting can briefly become negative while a concurrent free saturates its counter.
        query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
        /// Remaining headroom becomes zero once current query usage reaches the spill threshold.
        const UInt64 available_memory
            = max_bytes_before_external - std::min<UInt64>(max_bytes_before_external, query_memory_usage);
        /// Checking workspace before subtracting it avoids unsigned underflow or an overflowing sum.
        return workspace <= available_memory && growth <= available_memory - workspace;
    };

    chassert(!output_chunk);
    const size_t input_rows = input_chunk.getNumRows();
    size_t processed_rows = 0;

    /// The bulk estimate assumes every row adds a key. It includes table resize peaks, growth of the
    /// arena storing string keys, and other per-group state.
    /// Filtering releases its masks and packed keys before spilling; `spill_memory` already includes
    /// any pending filtered output. The budget covers the larger workspace alongside possible filter-state growth.
    /// A state that stays below the spill floor with this growth keeps hashing whatever the query memory.
    auto can_hash_whole_chunk = [&]
    {
        if (!max_bytes_before_external)
            return true;
        const size_t growth = hashing.filter.estimateGrowthMemory(input_chunk);
        return isBelowSpillFloor(hashing.filter.getTotalByteCount(), growth)
            || check_memory_budget(growth, std::max(filtering_memory, spill_memory));
    };
    if (can_hash_whole_chunk())
    {
        output_chunk = hashing.filter.filter(std::move(input_chunk));
        processed_rows = input_rows;
    }
    else
    {
        /// A failed bulk estimate does not imply that the actual new keys exceed the budget. A chunk can
        /// contain only existing keys, or enough repeated keys to avoid the projected table resize or string
        /// storage growth. Checking membership before budgeting each new key can therefore avoid a permanent
        /// switch to external processing, particularly for chunks with many repeated keys near the threshold.
        ///
        /// Checked insertion keeps the original input until the stopping row is known. Reserve an
        /// additional copy for cutting its unprocessed suffix while the filtered output stays alive.
        /// Before examining keys, require room for preparing the whole chunk and for starting a spill;
        /// individual new keys are checked for storage growth below.
        if (check_memory_budget(0, std::max(filtering_memory, input_bytes + spill_memory)))
        {
            auto can_insert = [&](size_t required_growth)
            {
                /// The current row masks and packed keys are already charged by this point. Each new
                /// key must leave room for cutting the suffix and preparing a spill after insertion.
                return check_memory_budget(required_growth, input_bytes + spill_memory);
            };
            auto result = hashing.filter.filterWithInsertionCheck(input_chunk.clone(), can_insert);
            processed_rows = result.processed_rows;
            output_chunk = std::move(result.chunk);
            if (processed_rows == input_rows)
                input_chunk.clear();
        }
    }

    consumed_rows += processed_rows;
    filter_memory.report(output_chunk, outputs.front().getHeader(), hashing.getRetainedFilterBytes());
    addResultRows(output_chunk.getNumRows());

    /// A hint or a size limit in the 'break' overflow mode retains this final result chunk.
    if ((params.limit_hint && result_rows >= params.limit_hint) || Traits::isLimitReached(hashing.filter))
    {
        state.template emplace<Finishing>();
        return;
    }

    if (processed_rows < input_rows)
    {
        if (processed_rows)
        {
            auto columns = input_chunk.detachColumns();
            for (auto & column : columns)
                column = column->cut(processed_rows, input_rows - processed_rows);
            input_chunk.setColumns(std::move(columns), input_rows - processed_rows);
        }

        LOG_TRACE(log, "Switching {} to external mode: {} "
            "(query memory: {}, spill threshold: {}, "
            "estimated peak extra memory for growth: {}, filtering and spill workspace: {}, "
            "processed rows in current chunk: {}, remaining rows: {})",
            Traits::operator_name,
            query_memory_usage > max_bytes_before_external
                ? "query memory exceeded the spill threshold"
                : "projected allocations exceed the remaining spill-threshold budget",
            formatReadableSizeWithBinarySuffix(query_memory_usage),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external),
            formatReadableSizeWithBinarySuffix(growth_memory),
            formatReadableSizeWithBinarySuffix(workspace_memory),
            processed_rows, input_rows - processed_rows);

        startSpilling(hashing);
        return;
    }

    /// Actual allocations and concurrent operators can consume more than the pre-insertion estimate.
    const Int64 query_memory_usage_after_insert = getCurrentQueryMemoryUsage();
    if (max_bytes_before_external && !isBelowSpillFloor(hashing.filter.getTotalByteCount(), 0)
        && query_memory_usage_after_insert > static_cast<Int64>(max_bytes_before_external))
    {
        LOG_TRACE(log, "Switching {} to external mode: query memory exceeded the spill threshold after insertion "
            "(query memory: {}, spill threshold: {})",
            Traits::operator_name,
            formatReadableSizeWithBinarySuffix(query_memory_usage_after_insert),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external));
        startSpilling(hashing);
    }
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::startSpilling(Hashing & hashing)
{
    chassert(!spill_layout);
    spill_layout.emplace(inputs.front().getSharedHeader(), hashing.filter.getKeyColumnsPositions(),
        hashing.filter.getKeyRepresentation(), attachesArrivalNumbers(), params.group_limit_end);

    if (hashing.filter.getTotalRowCount())
    {
        LOG_TRACE(log, "Extracting {} {} seen groups (state memory: {}) into sorted runs",
            hashing.filter.getTotalRowCount(), Traits::operator_name, formatReadableSizeWithBinarySuffix(hashing.filter.getTotalByteCount()));
        auto extractor = std::move(hashing.filter).extractSeenGroups();
        auto & extracting = state.template emplace<ExtractingSeenGroups>(std::move(extractor));
        /// The extractor retains the grouping state, but the dictionary filtering state is released.
        filter_memory.update(extracting.extractor->getTotalByteCount());
        extractSeenRun(extracting);
    }
    else
    {
        state.template emplace<CollectingInput>();
        filter_memory.update(0);
        LOG_TRACE(log, "{} hash table is empty; collecting input for input spill runs", Traits::operator_name);
    }
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::extractSeenRun(ExtractingSeenGroups & extracting)
{
    Chunks chunks;
    size_t bytes = 0;

    /// Bound working columns to one run while the extractor retains grouping state. A complete
    /// key can exceed the byte target, and sorting needs additional temporary buffers.
    while (!isCancelled() && bytes < DEFAULT_BYTES_IN_RUN)
    {
        auto seen = extracting.extractor->next(maxRowsInSortingUnit(), DEFAULT_BYTES_IN_RUN - bytes);
        filter_memory.update(extracting.extractor->getTotalByteCount());
        if (seen.keys.empty())
            break;

        auto chunk = spill_layout->prepareSeenChunk(std::move(seen));
        Block block = spill_layout->getSeenRunHeader()->cloneWithColumns(chunk.detachColumns());
        /// The writer merges by the complete run description. Binary-distinct keys can compare equal while
        /// carrying different seen counts, so those counts must also participate in sorting. Stable sorting
        /// preserves the order of binary representatives when both their keys and counts compare equal.
        sortBlock(block, spill_layout->getRunSortDescription(), /*limit=*/ 0, IColumn::PermutationSortStability::Stable);
        const auto rows = block.rows();
        Chunk sorted(block.detachColumns(), rows);
        bytes += sorted.allocatedBytes();
        chunks.push_back(std::move(sorted));
    }

    if (isCancelled())
        return;

    if (chunks.empty())
    {
        state.template emplace<CollectingInput>();
        LOG_TRACE(log, "Finished writing {} {} seen runs; hash table released, collecting input "
            "(query memory: {})", temporary_files_num, Traits::operator_name,
            formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()));
        return;
    }

    auto run = prepareRun(spill_layout->getSeenRunHeader(), std::move(chunks), bytes,
        spill_layout->getRunSortDescription(), MergeSorter::Mode::PreserveRows);
    auto extractor = std::move(extracting.extractor);
    auto & connecting = state.template emplace<ConnectingSeenRun>(std::move(run), std::move(extractor));
    FailPointInjection::pauseFailPoint(FailPoints::external_group_limit_seen_run_prepared_pause);
    readRun(connecting.run.progress);
}

template <typename Traits>
size_t ExternalGroupLimitTransform<Traits>::maxRowsInSortingUnit() const
{
    return std::max<size_t>(DEFAULT_BLOCK_SIZE, max_block_size_rows);
}

template <typename Traits>
bool ExternalGroupLimitTransform<Traits>::fitsSortingBudget(size_t rows, size_t column_bytes, size_t additional_input_bytes) const
{
    if (!max_bytes_before_external)
        return true;

    /// Buffered source chunks are already tracked; future input needs a separate allowance. Reserve
    /// space for copy-on-write mutation, column concatenation, and the sorted output while upstream
    /// owners may retain the source columns.
    const size_t workspace = additional_input_bytes + 3 * column_bytes + estimateSortingWorkspace(rows);
    const UInt64 query_memory = std::max<Int64>(0, getCurrentQueryMemoryUsage());
    return query_memory < max_bytes_before_external
        && workspace <= max_bytes_before_external - query_memory;
}

template <typename Traits>
bool ExternalGroupLimitTransform<Traits>::canAppendToSortingUnit(const SortingUnit & unit, size_t rows, size_t bytes) const
{
    return unit.rows + rows <= maxRowsInSortingUnit()
        && unit.allocated_bytes + bytes <= MAX_BYTES_IN_SORTING_UNIT
        && fitsSortingBudget(unit.rows + rows, unit.allocated_bytes + bytes);
}

template <typename Traits>
bool ExternalGroupLimitTransform<Traits>::canStartCoalescing(size_t rows, size_t bytes) const
{
    /// Start coalescing only with room for the target unit, including input that has not arrived.
    /// Otherwise, short units repeatedly pay for copying and sorting without removing enough
    /// cross-chunk rows to offset that work. Average row size predicts the remaining input;
    /// subsequent chunks still check their actual allocation before joining the unit.
    const size_t average_row_bytes = std::max<size_t>(1, bytes / rows);
    const size_t target_rows = std::max(rows, std::min(maxRowsInSortingUnit(), MAX_BYTES_IN_SORTING_UNIT / average_row_bytes));
    const size_t target_bytes = std::max(bytes, target_rows * average_row_bytes);
    return fitsSortingBudget(target_rows, target_bytes, target_bytes - bytes);
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::flushSortingUnit(CollectingInput & collecting)
{
    auto & pending = collecting.pending;
    if (pending.chunks.empty())
        return;

    const auto & header = spill_layout->getInputRunHeader();
    Chunk chunk;
    if (pending.chunks.size() == 1)
        chunk = std::move(pending.chunks.front());
    else
        chunk = Squashing::squashWithoutChunkInfo(std::move(pending.chunks));
    chassert(chunk.getNumRows() == pending.rows);
    pending.chunks.clear();
    pending.rows = 0;
    pending.allocated_bytes = 0;

    Block block = header->cloneWithColumns(chunk.detachColumns());

    /// `Squashing` retains input order. Stable sorting therefore keeps the earliest rows of each key across
    /// chunk boundaries and moves each row's arrival number and other service columns with it. Rows past
    /// the window end of their key can never be emitted, so they are dropped here.
    sortBlockAndLimitGroupRows(
        block, spill_layout->getKeySortDescription(), params.group_limit_end, IColumn::PermutationSortStability::Stable);
    const auto rows = block.rows();
    Chunk sorted(block.detachColumns(), rows);
    collecting.sorted_bytes += sorted.allocatedBytes();
    collecting.sorted_rows += rows;

    /// The group limit can increase the average width by removing repeated narrow rows. Allow one byte
    /// for division rounding and the width of the seen count that the final merge adds to input rows.
    const size_t average_row_bytes = sorted.bytes() / rows + 1 + spill_layout->getSeenRowsBytes();
    max_average_row_bytes = std::max(max_average_row_bytes, average_row_bytes);
    collecting.sorted_chunks.push_back(std::move(sorted));
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::collectInput(CollectingInput & collecting)
{
    auto chunk = std::move(input_chunk);
    /// Rowless marker chunks are dropped here: after spilling, the rows are emitted only once the input
    /// ends, so a forwarded marker (e.g. a watermark) would overtake the rows that arrived before it.
    if (unlikely(!chunk.hasRows()))
        return;

    /// Spill sorting reorders and removes rows, so source chunk metadata does not describe its
    /// output. Discard it before buffering and coalescing the row data.
    chunk.getChunkInfos().clear();

    const UInt64 first_arrival_number = consumed_rows;
    consumed_rows += chunk.getNumRows();
    auto prepared = spill_layout->prepareInputChunk(std::move(chunk), first_arrival_number);
    const size_t rows = prepared.getNumRows();
    const size_t bytes = prepared.allocatedBytes();

    /// Reserve the average materialized width before buffering. Allow one byte for division rounding
    /// and the width of the seen count that the final merge adds to input rows.
    const size_t average_row_bytes = prepared.bytes() / rows + 1 + spill_layout->getSeenRowsBytes();
    max_average_row_bytes = std::max(max_average_row_bytes, average_row_bytes);

    auto & pending = collecting.pending;

    /// Flush the previous unit before an input would exceed its targets or leave insufficient room
    /// for coalescing and sorting. A single large input is processed on its own to make progress.
    if (!pending.chunks.empty() && !canAppendToSortingUnit(pending, rows, bytes))
        flushSortingUnit(collecting);

    const bool can_coalesce = !pending.chunks.empty() || canStartCoalescing(rows, bytes);
    pending.rows += rows;
    pending.allocated_bytes += bytes;
    pending.chunks.push_back(std::move(prepared));

    const bool first_run = temporary_files_num == 0;
    const bool unit_full = pending.rows >= maxRowsInSortingUnit() || pending.allocated_bytes >= MAX_BYTES_IN_SORTING_UNIT;

    /// Base the spill decision on the sorting peak. Flushing resets the pending unit and can free
    /// memory through the group limit, so checking only afterward would lose that pressure.
    const bool sorting_budget_exceeded = !fitsSortingBudget(pending.rows, pending.allocated_bytes);

    /// A run can need spilling at EOF to make room for file readers. Reserve its write workspace
    /// while collecting, before the retained columns consume the budget needed to release them.
    const size_t run_rows = collecting.sorted_rows + pending.rows;
    const size_t run_bytes = collecting.sorted_bytes + pending.allocated_bytes;
    const size_t write_memory = estimateRunWriteMemory(run_rows, run_bytes);
    const size_t query_memory = std::max<Int64>(0, getCurrentQueryMemoryUsage());
    const bool write_budget_exceeded = max_bytes_before_external
        && query_memory + write_memory > max_bytes_before_external;
    const bool spill_budget_exceeded = sorting_budget_exceeded || write_budget_exceeded;
    if (first_run || spill_budget_exceeded || !can_coalesce || unit_full)
        flushSortingUnit(collecting);

    /// An empty hash table produces no seen files, so the first input chunk starts a run.
    /// Later runs keep a size floor when other operators consume the budget. The workspace check
    /// includes sorting copies and the workspace needed to write the accumulated run.
    if (first_run || (collecting.sorted_bytes >= minBytesInRun() && spill_budget_exceeded))
    {
        chassert(pending.chunks.empty());
        auto run = prepareRun(spill_layout->getInputRunHeader(), std::move(collecting.sorted_chunks), collecting.sorted_bytes,
            spill_layout->getKeySortDescription(), MergeSorter::Mode::LimitGroupRows);
        auto & connecting = state.template emplace<ConnectingInputRun>(std::move(run));
        readRun(connecting.run.progress);
    }
}

template <typename Traits>
typename ExternalGroupLimitTransform<Traits>::PreparedRun ExternalGroupLimitTransform<Traits>::prepareRun(
    SharedHeader run_header, Chunks chunks, size_t bytes, const SortDescription & description, MergeSorter::Mode mode)
{
    ++temporary_files_num;

    LOG_TRACE(log, "Will dump {} {} run #{} to disk "
        "(chunks: {}, buffered memory: {}, query memory: {}, spill threshold: {})",
        Traits::operator_name, mode == MergeSorter::Mode::PreserveRows ? "seen" : "input",
        temporary_files_num, chunks.size(),
        formatReadableSizeWithBinarySuffix(bytes),
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external));

    /// Reserving the run's space also preserves the configured amount of free disk space.
    TemporaryBlockStreamHolder tmp_stream(run_header, tmp_data, bytes + min_free_disk_space);
    /// The final merge applies the hint after the seen rows, which can remove rows of input runs.
    auto merger = std::make_unique<MergeSorter>(
        run_header, std::move(chunks), description, max_block_size_rows, /*limit=*/ 0, mode, preferred_block_bytes,
        params.group_limit_end);
    auto sink = std::make_shared<BufferingToFileSink>(run_header, std::move(tmp_stream), log);
    auto source = std::make_shared<BufferingFromFileSource>(run_header, sink->getHolder(), log);
    PreparedRun run{
        .progress = {std::move(merger), {}},
        .sink = std::move(sink),
        .source = std::move(source),
        .initial_merge = {},
    };
    if (!merge_registration && spilled_runs.empty())
        run.initial_merge = prepareMerge();

    return run;
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::readRun(RunWriteProgress & progress)
{
    chassert(progress.merger);
    chassert(!progress.chunk);
    /// Reads can consume only dropped rows and return zero rows. Skip those chunks while retaining
    /// cancellation checks between reads, and finish writing only when the merger is exhausted.
    while (!isCancelled())
    {
        progress.chunk = progress.merger->read();
        if (!progress.chunk)
            break;

        if (progress.chunk.hasRows())
        {
            progress.max_block_bytes = std::max<size_t>(progress.max_block_bytes, progress.chunk.allocatedBytes());

            /// Written blocks have materialized seen counts. Allow one byte for division rounding; merging
            /// can change the average width again when it drops rows across sorted chunks.
            max_average_row_bytes = std::max<size_t>(max_average_row_bytes, progress.chunk.bytes() / progress.chunk.getNumRows() + 1);
            return;
        }
    }

    estimated_file_read_memory += estimateRunReadMemory(
        progress.max_block_bytes, tmp_data->getSettings().buffer_size);
    progress.merger.reset();
}

template <typename Traits>
size_t ExternalGroupLimitTransform<Traits>::estimateRunWriteMemory(size_t rows, size_t allocated_bytes) const
{
    const size_t block_rows = MergeSorter::calculateMaxMergedBlockSize(
        max_block_size_rows, preferred_block_bytes, rows, allocated_bytes);

    /// Output blocks can coexist in the producer and sink. Allow twice each block's logical size for
    /// column capacity growth.
    const size_t output_memory = 4 * max_average_row_bytes * block_rows;

    /// The temporary writer also needs file, compression-input, and compression-output buffers.
    const size_t write_buffers_memory = 3 * tmp_data->getSettings().buffer_size;
    return output_memory + write_buffers_memory;
}

template <typename Traits>
size_t ExternalGroupLimitTransform<Traits>::selectTailSpillPrefix(const CollectingInput & collecting) const
{
    const auto & chunks = collecting.sorted_chunks;
    if (!max_bytes_before_external || chunks.empty())
        return 0;

    const size_t query_memory = std::max<Int64>(0, getCurrentQueryMemoryUsage());

    /// Input blocks and the tail are already charged. Reserve output columns and copies across
    /// processor ports separately from file readers. Average row sizes are rounded upward, and the
    /// multiplier allows allocation growth; this remains an estimate for uneven and oversized values.
    const size_t output_memory = 4 * max_average_row_bytes * max_block_size_rows;
    const size_t merge_memory = estimated_file_read_memory + output_memory;
    if (query_memory + merge_memory <= max_bytes_before_external)
        return 0;

    /// Equal keys keep their earliest input rows. Spilling a prefix preserves that precedence when
    /// the new file is registered before the retained suffix; arbitrary subsets would not.
    size_t prefix_bytes = 0;
    size_t prefix_rows = 0;
    for (size_t prefix = 0; prefix < chunks.size(); ++prefix)
    {
        prefix_bytes += chunks[prefix].allocatedBytes();
        prefix_rows += chunks[prefix].getNumRows();

        /// Spilling a prefix replaces its columns with another file reader. Use the writer's block
        /// sizing rule and the largest observed average row width, allowing for column capacity
        /// rounding, to budget that replacement.
        const size_t block_rows = MergeSorter::calculateMaxMergedBlockSize(
            max_block_size_rows, preferred_block_bytes, prefix_rows, prefix_bytes);
        const size_t new_reader_memory = estimateRunReadMemory(
            2 * max_average_row_bytes * block_rows, tmp_data->getSettings().buffer_size);
        if (query_memory + merge_memory + new_reader_memory <= max_bytes_before_external + prefix_bytes)
            return prefix + 1;
    }

    /// Release the entire tail when no suffix fits. File readers can themselves exceed the soft
    /// threshold, so removing the tail does not impose a hard bound on merge memory.
    return chunks.size();
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::prepareTail(PreparingTail & tail)
{
    flushSortingUnit(tail.collecting);
    auto & chunks = tail.collecting.sorted_chunks;
    const size_t prefix_size = selectTailSpillPrefix(tail.collecting);
    if (prefix_size)
    {
        Chunks prefix;
        prefix.reserve(prefix_size);
        size_t prefix_bytes = 0;
        size_t prefix_rows = 0;
        for (size_t i = 0; i < prefix_size; ++i)
        {
            prefix_bytes += chunks[i].allocatedBytes();
            prefix_rows += chunks[i].getNumRows();
            prefix.push_back(std::move(chunks[i]));
        }
        chunks.erase(chunks.begin(), chunks.begin() + prefix_size);
        tail.collecting.sorted_bytes -= prefix_bytes;
        tail.collecting.sorted_rows -= prefix_rows;
        ProfileEvents::increment(Traits::tailSpilledRowsEvent(), prefix_rows);

        LOG_TRACE(log, "Spilling a {} tail prefix before merging "
            "(chunks: {}, bytes: {}, remaining chunks: {}, remaining bytes: {}, "
            "estimated file-reader memory: {}, query memory: {}, spill threshold: {})",
            Traits::operator_name, prefix_size, formatReadableSizeWithBinarySuffix(prefix_bytes), chunks.size(),
            formatReadableSizeWithBinarySuffix(tail.collecting.sorted_bytes),
            formatReadableSizeWithBinarySuffix(estimated_file_read_memory),
            formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external));

        auto run = prepareRun(spill_layout->getInputRunHeader(), std::move(prefix), prefix_bytes,
            spill_layout->getKeySortDescription(), MergeSorter::Mode::LimitGroupRows);
        auto remaining = std::move(tail.collecting);
        auto & connecting = state.template emplace<ConnectingTailRun>(std::move(run), std::move(remaining));
        readRun(connecting.run.progress);
        return;
    }

    ProfileEvents::increment(Traits::mergeEvent());
    ProfileEvents::increment(Traits::tailKeptRowsEvent(), tail.collecting.sorted_rows);
    LOG_TRACE(log, "Preparing final {} merge "
        "(temporary runs: {}, in-memory chunks: {}, restore input order: {})",
        Traits::operator_name, temporary_files_num, tail.collecting.sorted_chunks.size(),
        !preserve_input_order ? "no" : (attachesArrivalNumbers() ? "by arrival numbers" : "by sort columns"));

    /// Register the final input even when the tail is empty, then close merge-input registration.
    /// The tail is merged under the same group limit as input runs on disk.
    auto source = std::make_shared<MergeSorterSource>(
        spill_layout->getInputRunHeader(), std::move(tail.collecting.sorted_chunks), spill_layout->getKeySortDescription(),
        max_block_size_rows, /*limit=*/ 0, MergeSorter::Mode::LimitGroupRows, preferred_block_bytes, params.group_limit_end);
    state.template emplace<ConnectingTail>(std::move(source));
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::consumeMerged(Merging & merging)
{
    auto chunk = std::move(merging.chunk);
    if (!chunk.hasRows())
        return;

    /// Arrival numbers have served their purpose after the optional order-restoration sort.
    chassert(!output_chunk);
    output_chunk = spill_layout->restoreOutputChunk(std::move(chunk));
    filter_memory.report(output_chunk, outputs.front().getHeader(), 0);
    addResultRows(output_chunk.getNumRows());

    /// The row limit applies to the result. This stream's hash table has been released, so no local state
    /// memory remains to check against the byte limit. The row limit is checked before applying the hint.
    if (!params.size_limits.check(result_rows, /*bytes=*/ 0, Traits::operator_name, ErrorCodes::SET_SIZE_LIMIT_EXCEEDED)
        || (params.limit_hint && result_rows >= params.limit_hint))
        state.template emplace<Finishing>();
}

template <typename Traits>
typename ExternalGroupLimitTransform<Traits>::PreparedMerge ExternalGroupLimitTransform<Traits>::prepareMerge()
{
    const auto & merged_header = spill_layout->getMergedHeader();

    /// The merger cannot consume its inputs until the final in-memory tail has been registered.
    PreparedMerge prepared;
    prepared.merger = std::make_shared<GroupLimitSortedTransform>(
        String(Traits::merge_transform_name), SharedHeaders{}, merged_header, spill_layout->getKeySortDescription(),
        spill_layout->getSeenRowsColumn(), params.group_offset, params.group_limit_end, max_block_size_rows,
        /*have_all_inputs=*/ false);

    if (preserve_input_order)
    {
        const auto & restoration_description
            = attachesArrivalNumbers() ? spill_layout->getArrivalNumberSortDescription() : input_order_description;

        /// Restore the input order under the same spill policy as input runs. The group limit leaves only
        /// result rows, so the sort can safely retain a hinted prefix. Rows admitted during hashing already
        /// form the result's prefix and reduce the remaining hint.
        chassert(!params.limit_hint || result_rows < params.limit_hint);
        const UInt64 remaining_limit_hint = params.limit_hint ? params.limit_hint - result_rows : 0;
        prepared.order_restoration.emplace_back(
            std::make_shared<PartialSortingTransform>(merged_header, restoration_description, remaining_limit_hint));

        /// Remerge at the run-size threshold before considering another spill. The sorter stops
        /// remerging when it cannot halve retained memory, avoiding repeated unproductive merges.
        prepared.order_restoration.emplace_back(std::make_shared<MergeSortingTransform>(
            merged_header,
            restoration_description,
            max_block_size_rows,
            preferred_block_bytes,
            remaining_limit_hint,
            /*increase_sort_description_compile_attempts=*/ false,
            minBytesInRun(),
            /*remerge_lowered_memory_bytes_ratio_=*/ 2.,
            max_bytes_before_external ? minBytesInRun() : 0,
            max_bytes_before_external,
            tmp_data,
            min_free_disk_space));
    }

    return prepared;
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::connectMerge(PreparedMerge & prepared, Processors & processors)
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

    inputs.emplace_back(*spill_layout->getMergedHeader(), this);
    connect(*output, inputs.back());
    merge_registration.emplace(std::move(prepared.merger), inputs.back());
}

template <typename Traits>
void ExternalGroupLimitTransform<Traits>::connectRunSource(PreparedRun & prepared, Processors & processors)
{
    if (prepared.initial_merge)
        connectMerge(*prepared.initial_merge, processors);

    chassert(merge_registration);
    auto & merger = *merge_registration->merger;
    merger.addInput(prepared.source->getPort().getHeader());
    connect(prepared.source->getPort(), merger.getInputs().back());
    processors.emplace_back(prepared.source);
}

template <typename Traits>
OutputPort & ExternalGroupLimitTransform<Traits>::connectRun(PreparedRun & prepared, Processors & processors)
{
    connectRunSource(prepared, processors);
    outputs.emplace_back(prepared.sink->getPort().getHeader(), this);
    connect(outputs.back(), prepared.sink->getPort());
    processors.emplace_back(prepared.sink);
    return outputs.back();
}

template <typename Traits>
IProcessor::PipelineUpdate ExternalGroupLimitTransform<Traits>::updatePipeline()
{
    Processors processors;
    if (!spilled_runs.empty())
    {
        for (auto & run : spilled_runs)
            connectRunSource(run, processors);
        spilled_runs.clear();
        return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
    }

    std::visit([this, &processors]<typename Phase>(Phase & phase)
    {
        if constexpr (std::is_same_v<Phase, ConnectingSeenRun> || std::is_same_v<Phase, ConnectingTailRun>)
        {
            auto run = std::move(phase.run);
            auto & output = connectRun(run, processors);

            /// Seen-key extraction waits for file finalization before preparing another run;
            /// a tail prefix waits before budgeting the suffix. Both relay the reader's completion
            /// dependency only after that wait finishes.
            inputs.emplace_back(Block(), this);
            auto & completion = inputs.back();
            connect(run.sink->getCompletionPort(), completion);
            outputs.emplace_back(Block(), this);
            auto & readiness = outputs.back();
            connect(readiness, run.source->getCompletionPort());
            if constexpr (std::is_same_v<Phase, ConnectingSeenRun>)
            {
                auto extractor = std::move(phase.extractor);
                state.template emplace<WritingSeenRun>(std::move(run.progress), output, std::move(extractor), completion, readiness);
            }
            else
            {
                auto remaining = std::move(phase.remaining);
                state.template emplace<WritingTailRun>(std::move(run.progress), output, std::move(remaining), completion, readiness);
            }
        }
        else if constexpr (std::is_same_v<Phase, ConnectingInputRun>)
        {
            auto run = std::move(phase.run);
            auto & output = connectRun(run, processors);
            /// Input resumes after handoff; the reader waits directly for file completion.
            connect(run.sink->getCompletionPort(), run.source->getCompletionPort());
            state.template emplace<WritingInputRun>(std::move(run.progress), output);
        }
        else if constexpr (std::is_same_v<Phase, ConnectingTail>)
        {
            chassert(merge_registration);
            auto source = std::move(phase.source);
            auto & merger = *merge_registration->merger;
            merger.addInput(*spill_layout->getInputRunHeader());
            connect(source->getPort(), merger.getInputs().back());
            merger.setHaveAllInputs();
            auto & input = merge_registration->input;
            merge_registration.reset();
            processors.emplace_back(std::move(source));
            state.template emplace<Merging>(input, Chunk{});
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "External {} has no pipeline update in state {}", Traits::operator_name, state.index());
    }, state);

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

template class ExternalGroupLimitTransform<ExternalDistinctTraits>;
template class ExternalGroupLimitTransform<ExternalLimitByTraits>;

}
