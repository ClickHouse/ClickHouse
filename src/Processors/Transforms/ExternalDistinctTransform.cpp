#include <Processors/Transforms/ExternalDistinctTransform.h>

#include <algorithm>
#include <type_traits>

#include <Interpreters/Squashing.h>
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
    extern const Event ExternalDistinctTailSpilledRows;
    extern const Event ExternalDistinctTailKeptRows;
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

/// Sorting units combine small input chunks before removing duplicates. Their row and byte targets
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

size_t estimateSortingWorkspace(size_t rows)
{
    /// A sorting permutation stores each row's original index. Include array padding and the
    /// power-of-two capacity rounding, rather than counting only the indices themselves.
    using Permutation = IColumn::Permutation;
    const size_t permutation_bytes = roundUpToPowerOfTwoOrZero(PODArrayDetails::minimum_memory_for_elements(
        rows, sizeof(Permutation::value_type), Permutation::pad_left, Permutation::pad_right));

    /// Numeric radix sorting holds two value-index arrays alongside the permutation. Each pair can
    /// occupy twice an index's size, and the histograms need up to 8 KiB. This allowance also covers
    /// the equal-key ranges used by comparison sorting and duplicate removal.
    return 5 * permutation_bytes + (8 << 10);
}

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
    size_t preferred_block_bytes_,
    bool preserve_input_order_)
    : IProcessor({header_}, {header_})
    , state(std::in_place_type<Hashing>, *header_, columns_, set_size_limits_)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , max_bytes_before_external_distinct(max_bytes_before_external_distinct_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_size_rows(max_block_size_rows_)
    , preferred_block_bytes(preferred_block_bytes_)
    , preserve_input_order(preserve_input_order_)
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
            || std::is_same_v<Phase, ConnectingInputRun> || std::is_same_v<Phase, ConnectingTailRun>
            || std::is_same_v<Phase, ConnectingTail>)
            return Status::UpdatePipeline;
        else if constexpr (std::is_same_v<Phase, WritingSuppressionRun>)
            return prepareSuppressionWrite(phase);
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
        auto remaining = std::move(collecting);
        state.emplace<PreparingTail>(std::move(remaining));
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

IProcessor::Status ExternalDistinctTransform::prepareTailWrite(WritingTailRun & writing)
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
    state.emplace<PreparingTail>(std::move(remaining));
    return Status::Ready;
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
        else if constexpr (std::is_same_v<Phase, WritingSuppressionRun>
            || std::is_same_v<Phase, WritingInputRun> || std::is_same_v<Phase, WritingTailRun>)
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

    /// The input columns and existing set are already charged to query memory. The estimates below
    /// cover additional allocations for inserting keys, filtering rows, and preparing the first spill run.
    ///
    /// Filtering can allocate masks marking rows to keep, packed keys combining multiple key columns
    /// into one value per row, and output copies. The masks and packed keys are released before spilling.
    const size_t filtering_memory = hashing.set.estimateFilteringMemory(input_chunk);

    /// The prepared input is already charged to query memory. Its allocated size estimates the cost
    /// of another copy when filtering, sorting, or cutting the unprocessed part of a chunk.
    const size_t input_bytes = input_chunk.allocatedBytes();

    /// Service columns store internal spill metadata: fingerprints for keys compared by hash and
    /// arrival numbers recording each row's original position. These columns are added when the key
    /// representation needs fingerprints or the result must preserve input order.
    const size_t service_columns_bytes = DistinctSpillLayout::estimateServiceColumnsMemory(
        input_chunk.getNumRows(), hashing.set.getKeyRepresentation(), preserve_input_order);

    /// A suppression run contains keys already accepted for output, allowing the final merge to
    /// discard later duplicates. Extraction targets `DEFAULT_BYTES_IN_RUN` bytes per run; twice that
    /// target covers extracted columns alongside their sorted copies. This is a soft target because
    /// one large key or a column allocation can exceed it.
    const size_t suppression_columns_bytes = 2 * DEFAULT_BYTES_IN_RUN;

    /// Ordinary runs contain input rows still to be deduplicated. Sorting can retain the input and
    /// service columns alongside their permuted copies. The input is already charged, so budget one
    /// input copy and both the original and copied service columns.
    const size_t ordinary_columns_bytes = input_bytes + 2 * service_columns_bytes;

    /// Suppression extraction prepares a sorting unit at a time; the first ordinary run can contain
    /// the whole input chunk. Reserve sorting indices and their temporary arrays for the larger input.
    const size_t sort_rows = std::max(maxRowsInSortingUnit(), input_chunk.getNumRows());
    const size_t sorting_workspace = estimateSortingWorkspace(sort_rows);

    /// Writing a temporary file can hold uncompressed input, compressed output, and a file buffer
    /// at the same time. The estimate allows three configured buffer sizes; oversized values and codec
    /// overhead can exceed it.
    const size_t write_buffers_bytes = 3 * tmp_data->getSettings().buffer_size;

    /// A filtered output copy can remain pending while suppression keys are extracted. The original
    /// input can still be shared upstream, so this output copy needs an additional `input_bytes`.
    /// Suppression extraction and ordinary sorting run separately; take the larger column estimate,
    /// then add allowances for sorting workspace and buffers for writing the file.
    const size_t spill_memory
        = std::max(input_bytes + suppression_columns_bytes, ordinary_columns_bytes) + sorting_workspace + write_buffers_bytes;

    /// These values retain the last check's tracked query usage, additional memory for new keys, and
    /// temporary workspace estimate so the spill log describes the check that rejected insertion.
    UInt64 query_memory_usage = 0;
    size_t growth_memory = 0;
    size_t workspace_memory = 0;
    auto check_memory_budget = [&](size_t growth, size_t workspace)
    {
        growth_memory = growth;
        workspace_memory = workspace;
        /// `max_bytes_before_external_distinct` applies to total query memory, so current usage reduces
        /// the budget for additional allocations.
        /// Query accounting can briefly become negative while a concurrent free saturates its counter.
        query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
        /// Remaining headroom becomes zero once current query usage reaches the spill threshold.
        const UInt64 available_memory
            = max_bytes_before_external_distinct - std::min<UInt64>(max_bytes_before_external_distinct, query_memory_usage);
        /// Checking workspace before subtracting it avoids unsigned underflow or an overflowing sum.
        return workspace <= available_memory && growth <= available_memory - workspace;
    };

    chassert(!output_chunk);
    const size_t input_rows = input_chunk.getNumRows();
    size_t processed_rows = 0;

    /// The bulk estimate assumes every row adds a key. It includes table resize peaks, growth of the
    /// arena storing string keys, and retained bitmaps marking seen `LowCardinality` dictionary entries.
    /// Filtering releases its masks and packed keys before spilling; `spill_memory` already includes
    /// any pending filtered output. The budget covers the larger workspace alongside possible set growth.
    if (check_memory_budget(hashing.set.estimateGrowthMemory(input_chunk), std::max(filtering_memory, spill_memory)))
    {
        output_chunk = hashing.set.filter(std::move(input_chunk));
        processed_rows = input_rows;
    }
    else
    {
        /// A failed bulk estimate does not imply that the actual new keys exceed the budget. A chunk can
        /// contain only existing keys, or enough duplicates to avoid the projected table resize or string
        /// storage growth. Checking membership before budgeting each new key can therefore avoid a permanent
        /// switch to external processing, particularly for chunks with many duplicates near the threshold.
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
            auto result = hashing.set.filterWithInsertionCheck(input_chunk.clone(), can_insert);
            processed_rows = result.processed_rows;
            output_chunk = std::move(result.chunk);
            if (processed_rows == input_rows)
                input_chunk.clear();
        }
    }

    consumed_rows += processed_rows;
    result_rows += output_chunk.getNumRows();

    /// A hint or a size limit in the 'break' overflow mode retains this final result chunk.
    if ((limit_hint && result_rows >= limit_hint) || hashing.set.isLimitReached())
    {
        state.emplace<Finishing>();
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

        LOG_TRACE(log, "Switching DISTINCT to external mode: {} "
            "(query memory: {}, spill threshold: {}, "
            "estimated peak extra memory for growth: {}, filtering and spill workspace: {}, "
            "processed rows in current chunk: {}, remaining rows: {})",
            query_memory_usage > max_bytes_before_external_distinct
                ? "query memory exceeded the spill threshold"
                : "projected allocations exceed the remaining spill-threshold budget",
            formatReadableSizeWithBinarySuffix(query_memory_usage),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct),
            formatReadableSizeWithBinarySuffix(growth_memory),
            formatReadableSizeWithBinarySuffix(workspace_memory),
            processed_rows, input_rows - processed_rows);

        startSpilling(hashing);
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
        auto key_columns = extracting.keys->next(maxRowsInSortingUnit(), DEFAULT_BYTES_IN_RUN - bytes);
        if (key_columns.empty())
            break;

        auto chunk = spill_layout->prepareSuppressionChunk(std::move(key_columns));
        Block block = spill_layout->getSuppressionRunHeader()->cloneWithColumns(chunk.detachColumns());
        /// Stable sorting preserves binary representatives of sort-equivalent keys. The flag is
        /// constant within this chunk, so key order also satisfies the run order.
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

size_t ExternalDistinctTransform::maxRowsInSortingUnit() const
{
    return std::max<size_t>(DEFAULT_BLOCK_SIZE, max_block_size_rows);
}

bool ExternalDistinctTransform::fitsSortingBudget(size_t rows, size_t column_bytes, size_t additional_input_bytes) const
{
    /// Buffered source chunks are already tracked; future input needs a separate allowance. Reserve
    /// space for copy-on-write mutation, column concatenation, and the sorted output while upstream
    /// owners may retain the source columns.
    const size_t workspace = additional_input_bytes + 3 * column_bytes + estimateSortingWorkspace(rows);
    const UInt64 query_memory = std::max<Int64>(0, getCurrentQueryMemoryUsage());
    return query_memory < max_bytes_before_external_distinct
        && workspace <= max_bytes_before_external_distinct - query_memory;
}

bool ExternalDistinctTransform::canAppendToSortingUnit(const SortingUnit & unit, size_t rows, size_t bytes) const
{
    return unit.rows + rows <= maxRowsInSortingUnit()
        && unit.allocated_bytes + bytes <= MAX_BYTES_IN_SORTING_UNIT
        && fitsSortingBudget(unit.rows + rows, unit.allocated_bytes + bytes);
}

bool ExternalDistinctTransform::canStartCoalescing(size_t rows, size_t bytes) const
{
    /// Start coalescing only with room for the target unit, including input that has not arrived.
    /// Otherwise, short units repeatedly pay for copying and sorting without removing enough
    /// cross-chunk duplicates to offset that work. Average row size predicts the remaining input;
    /// subsequent chunks still check their actual allocation before joining the unit.
    const size_t average_row_bytes = std::max<size_t>(1, bytes / rows);
    const size_t target_rows = std::max(rows, std::min(maxRowsInSortingUnit(), MAX_BYTES_IN_SORTING_UNIT / average_row_bytes));
    const size_t target_bytes = std::max(bytes, target_rows * average_row_bytes);
    return fitsSortingBudget(target_rows, target_bytes, target_bytes - bytes);
}

void ExternalDistinctTransform::flushSortingUnit(CollectingInput & collecting)
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

    /// `Squashing` retains input order. Stable compaction therefore keeps the first payload across
    /// chunk boundaries and moves each row's arrival number and other service columns with it.
    sortBlockAndDeduplicate(block, spill_layout->getKeySortDescription(), IColumn::PermutationSortStability::Stable);
    const auto rows = block.rows();
    Chunk sorted(block.detachColumns(), rows);
    collecting.sorted_bytes += sorted.allocatedBytes();
    collecting.sorted_rows += rows;

    /// Deduplication can increase the average width by removing repeated narrow rows. Allow one byte
    /// for division rounding and another for the emitted flag, which is still constant here.
    const size_t average_row_bytes = sorted.bytes() / rows + 1 + sizeof(UInt8);
    max_average_row_bytes = std::max(max_average_row_bytes, average_row_bytes);
    collecting.sorted_chunks.push_back(std::move(sorted));
}

void ExternalDistinctTransform::collectInput(CollectingInput & collecting)
{
    auto chunk = std::move(input_chunk);
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
    /// and another for the emitted flag, which remains constant until the run is merged.
    const size_t average_row_bytes = prepared.bytes() / rows + 1 + sizeof(UInt8);
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
    /// memory through deduplication, so checking only afterward would lose that pressure.
    const bool sorting_budget_exceeded = !fitsSortingBudget(pending.rows, pending.allocated_bytes);

    /// A run can need spilling at EOF to make room for file readers. Reserve its write workspace
    /// while collecting, before the retained columns consume the budget needed to release them.
    const size_t run_rows = collecting.sorted_rows + pending.rows;
    const size_t run_bytes = collecting.sorted_bytes + pending.allocated_bytes;
    const size_t write_memory = estimateRunWriteMemory(run_rows, run_bytes);
    const size_t query_memory = std::max<Int64>(0, getCurrentQueryMemoryUsage());
    const bool write_budget_exceeded = query_memory + write_memory > max_bytes_before_external_distinct;
    const bool spill_budget_exceeded = sorting_budget_exceeded || write_budget_exceeded;
    if (first_run || spill_budget_exceeded || !can_coalesce || unit_full)
        flushSortingUnit(collecting);

    /// An empty hash set produces no suppression files, so the first ordinary chunk starts a run.
    /// Later runs keep a size floor when other operators consume the budget. The workspace check
    /// includes sorting copies and the workspace needed to write the accumulated run.
    if (first_run || (collecting.sorted_bytes >= minBytesInRun() && spill_budget_exceeded))
    {
        chassert(pending.chunks.empty());
        auto run = prepareRun(spill_layout->getInputRunHeader(), std::move(collecting.sorted_chunks), collecting.sorted_bytes,
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
        run_header, std::move(chunks), description, max_block_size_rows, /*limit=*/ 0, mode, preferred_block_bytes);
    auto sink = std::make_shared<BufferingToFileSink>(run_header, std::move(tmp_stream), log);
    auto source = std::make_shared<BufferingFromFileSource>(run_header, sink->getHolder(), log);
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
        {
            progress.max_block_bytes = std::max(progress.max_block_bytes, progress.chunk.allocatedBytes());

            /// Written blocks have materialized flags. Allow one byte for division rounding; merging
            /// can change the average width again when it removes duplicates across sorted chunks.
            max_average_row_bytes = std::max(max_average_row_bytes, progress.chunk.bytes() / progress.chunk.getNumRows() + 1);
            return;
        }
    }

    estimated_file_read_memory += estimateRunReadMemory(
        progress.max_block_bytes, tmp_data->getSettings().buffer_size);
    progress.merger.reset();
}

size_t ExternalDistinctTransform::estimateRunWriteMemory(size_t rows, size_t allocated_bytes) const
{
    /// The block-size calculation sees materialized flags, adding one byte to each buffered row.
    const size_t block_rows = MergeSorter::calculateMaxMergedBlockSize(
        max_block_size_rows, preferred_block_bytes, rows, allocated_bytes + rows);

    /// `MergeSorter` expands the emitted-row flags while the buffered inputs remain alive.
    /// Allow twice their logical size for allocation rounding.
    const size_t flag_columns_memory = 2 * rows;

    /// Output blocks can coexist in the producer and sink. Allow twice each block's logical size for
    /// column capacity growth.
    const size_t output_memory = 4 * max_average_row_bytes * block_rows;

    /// The temporary writer also needs file, compression-input, and compression-output buffers.
    const size_t write_buffers_memory = 3 * tmp_data->getSettings().buffer_size;
    return flag_columns_memory + output_memory + write_buffers_memory;
}

size_t ExternalDistinctTransform::selectTailSpillPrefix(const CollectingInput & collecting) const
{
    const auto & chunks = collecting.sorted_chunks;
    if (chunks.empty())
        return 0;

    const size_t query_memory = std::max<Int64>(0, getCurrentQueryMemoryUsage());

    /// Input blocks and the tail are already charged. Reserve output columns and copies across
    /// processor ports separately from file readers. Average row sizes are rounded upward, and the
    /// multiplier allows allocation growth; this remains an estimate for uneven and oversized values.
    const size_t output_memory = 4 * max_average_row_bytes * max_block_size_rows;

    /// `MergeSorter` expands the emitted-row flags before merging. Allow capacity rounding for
    /// these byte columns while the original tail chunks remain alive.
    const size_t merge_memory = estimated_file_read_memory + output_memory + 2 * collecting.sorted_rows;
    if (query_memory + merge_memory <= max_bytes_before_external_distinct)
        return 0;

    /// Equal keys keep their first input row. Spilling a prefix preserves that precedence when
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
            max_block_size_rows, preferred_block_bytes, prefix_rows, prefix_bytes + prefix_rows);
        const size_t new_reader_memory = estimateRunReadMemory(
            2 * max_average_row_bytes * block_rows, tmp_data->getSettings().buffer_size);
        const size_t released_bytes = prefix_bytes + 2 * prefix_rows;
        if (query_memory + merge_memory + new_reader_memory
            <= max_bytes_before_external_distinct + released_bytes)
            return prefix + 1;
    }

    /// Release the entire tail when no suffix fits. File readers can themselves exceed the soft
    /// threshold, so removing the tail does not impose a hard bound on merge memory.
    return chunks.size();
}

void ExternalDistinctTransform::prepareTail(PreparingTail & tail)
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
        ProfileEvents::increment(ProfileEvents::ExternalDistinctTailSpilledRows, prefix_rows);

        LOG_TRACE(log, "Spilling a DISTINCT tail prefix before merging "
            "(chunks: {}, bytes: {}, remaining chunks: {}, remaining bytes: {}, "
            "estimated file-reader memory: {}, query memory: {}, spill threshold: {})",
            prefix_size, formatReadableSizeWithBinarySuffix(prefix_bytes), chunks.size(),
            formatReadableSizeWithBinarySuffix(tail.collecting.sorted_bytes),
            formatReadableSizeWithBinarySuffix(estimated_file_read_memory),
            formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

        auto run = prepareRun(spill_layout->getInputRunHeader(), std::move(prefix), prefix_bytes,
            spill_layout->getKeySortDescription(), MergeSorter::Mode::MergeUniqueChunks);
        auto remaining = std::move(tail.collecting);
        auto & connecting = state.emplace<ConnectingTailRun>(std::move(run), std::move(remaining));
        readRun(connecting.run.progress);
        return;
    }

    ProfileEvents::increment(ProfileEvents::ExternalDistinctMerge);
    ProfileEvents::increment(ProfileEvents::ExternalDistinctTailKeptRows, tail.collecting.sorted_rows);
    LOG_TRACE(log, "Preparing final DISTINCT merge "
        "(temporary runs: {}, in-memory chunks: {}, restore input order: {})",
        temporary_files_num, tail.collecting.sorted_chunks.size(), spill_layout->preservesInputOrder());

    /// Register the final input even when the tail is empty, then close merge-input registration.
    /// The tail is merged into unique chunks under the same contract as ordinary disk runs.
    auto source = std::make_shared<MergeSorterSource>(
        spill_layout->getInputRunHeader(), std::move(tail.collecting.sorted_chunks), spill_layout->getKeySortDescription(),
        max_block_size_rows, /*limit=*/ 0, MergeSorter::Mode::MergeUniqueChunks, preferred_block_bytes);
    state.emplace<ConnectingTail>(std::move(source));
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

ExternalDistinctTransform::PreparedMerge ExternalDistinctTransform::prepareMerge()
{
    const auto & merged_header = spill_layout->getMergedHeader();

    /// The merger cannot consume its inputs until the final in-memory tail has been registered.
    PreparedMerge prepared;
    prepared.merger = std::make_shared<DistinctSortedTransform>(
        SharedHeaders{}, merged_header, spill_layout->getRunSortDescription(),
        max_block_size_rows, /*have_all_inputs=*/ false);

    if (spill_layout->preservesInputOrder())
    {
        const auto & arrival_number_description = spill_layout->getArrivalNumberSortDescription();

        /// Restore arrival order under the same spill policy as input runs. Deduplication and
        /// suppression leave only new distinct rows, so the sort can safely retain a hinted prefix.
        /// Rows admitted during hashing already form the result's prefix and reduce the remaining hint.
        chassert(!limit_hint || result_rows < limit_hint);
        const UInt64 remaining_limit_hint = limit_hint ? limit_hint - result_rows : 0;
        prepared.order_restoration.emplace_back(
            std::make_shared<PartialSortingTransform>(merged_header, arrival_number_description, remaining_limit_hint));

        /// Remerge at the run-size threshold before considering another spill. The sorter stops
        /// remerging when it cannot halve retained memory, avoiding repeated unproductive merges.
        prepared.order_restoration.emplace_back(std::make_shared<MergeSortingTransform>(
            merged_header,
            arrival_number_description,
            max_block_size_rows,
            preferred_block_bytes,
            remaining_limit_hint,
            /*increase_sort_description_compile_attempts=*/ false,
            minBytesInRun(),
            /*remerge_lowered_memory_bytes_ratio_=*/ 2.,
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

    inputs.emplace_back(*spill_layout->getMergedHeader(), this);
    connect(*output, inputs.back());
    merge_registration.emplace(std::move(prepared.merger), inputs.back());
}

OutputPort & ExternalDistinctTransform::connectRun(PreparedRun & prepared, Processors & processors)
{
    if (prepared.initial_merge)
        connectMerge(*prepared.initial_merge, processors);

    chassert(merge_registration);
    auto & merger = *merge_registration->merger;
    merger.addInput(prepared.source->getPort().getHeader());
    connect(prepared.source->getPort(), merger.getInputs().back());
    outputs.emplace_back(prepared.sink->getPort().getHeader(), this);
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
        if constexpr (std::is_same_v<Phase, ConnectingSuppressionRun> || std::is_same_v<Phase, ConnectingTailRun>)
        {
            auto run = std::move(phase.run);
            auto & output = connectRun(run, processors);

            /// Suppression extraction waits for file finalization before preparing another run;
            /// a tail prefix waits before budgeting the suffix. Both relay the reader's completion
            /// dependency only after that wait finishes.
            inputs.emplace_back(Block(), this);
            auto & completion = inputs.back();
            connect(run.sink->getCompletionPort(), completion);
            outputs.emplace_back(Block(), this);
            auto & readiness = outputs.back();
            connect(readiness, run.source->getCompletionPort());
            if constexpr (std::is_same_v<Phase, ConnectingSuppressionRun>)
            {
                auto keys = std::move(phase.keys);
                state.emplace<WritingSuppressionRun>(std::move(run.progress), output, std::move(keys), completion, readiness);
            }
            else
            {
                auto remaining = std::move(phase.remaining);
                state.emplace<WritingTailRun>(std::move(run.progress), output, std::move(remaining), completion, readiness);
            }
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
            merger.addInput(*spill_layout->getInputRunHeader());
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
