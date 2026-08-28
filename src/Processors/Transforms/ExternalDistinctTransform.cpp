#include <Processors/Transforms/ExternalDistinctTransform.h>

#include <algorithm>
#include <functional>

#include <Columns/ColumnsNumber.h>
#include <Core/SortCursor.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/SortingTransform.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>

namespace ProfileEvents
{
    extern const Event ExternalDistinctMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{

constexpr auto FLAG_COLUMN_NAME = "__distinct_already_emitted";

/// A run is written only when at least this much data was accumulated (but never more than the spill
/// threshold itself, so that tiny thresholds still spill deterministically). Without a floor, when it is
/// some *other* operator that keeps the memory usage of the query above the threshold, every consumed
/// chunk would be dumped as its own temporary file.
constexpr size_t MIN_BYTES_IN_RUN = DEFAULT_BLOCK_SIZE * 256;

SortDescription buildSortDescription(const Block & header, const ColumnNumbers & key_columns_pos)
{
    SortDescription description;
    description.reserve(key_columns_pos.size());
    for (const auto pos : key_columns_pos)
        description.emplace_back(header.getByPosition(pos).name, 1, 1);
    return description;
}

bool nonKeyColumnsAreRebuildable(const Block & header, const ColumnNumbers & key_columns_pos)
{
    std::vector<UInt8> is_key(header.columns(), 0);
    for (const auto pos : key_columns_pos)
        is_key[pos] = 1;

    for (size_t pos = 0; pos < header.columns(); ++pos)
    {
        if (is_key[pos])
            continue;

        const auto & column = header.getByPosition(pos).column;
        if (!column || !isColumnConst(*column))
            return false;
    }
    return true;
}

SharedHeader buildSpillHeader(const Block & header)
{
    /// The flag column only needs a name that is unique within the spill header (everything addresses
    /// it by position); a user column may legitimately be named like the flag, so uniquify by prepending
    /// underscores instead of failing.
    String flag_name = FLAG_COLUMN_NAME;
    while (header.has(flag_name))
        flag_name = "_" + flag_name;

    /// Constant (and other special) column representations cannot be written to temporary files in the
    /// Native format, and the input chunks are materialized before spilling anyway.
    Block spill_header = materializeBlock(header);
    auto flag_type = std::make_shared<DataTypeUInt8>();
    spill_header.insert({flag_type->createColumn(), flag_type, flag_name});
    return std::make_shared<const Block>(std::move(spill_header));
}

}

DistinctSortedFilter::DistinctSortedFilter(ColumnNumbers key_columns_pos_, SortDescription description_, size_t flag_column_pos_)
    : key_columns_pos(std::move(key_columns_pos_))
    , description(std::move(description_))
    , flag_column_pos(flag_column_pos_)
{
    chassert(key_columns_pos.size() == description.size());
    chassert(!key_columns_pos.empty());
}

void DistinctSortedFilter::reset()
{
    prev_chunk_latest_key.clear();
}

void DistinctSortedFilter::saveLatestKey(const ColumnRawPtrs & key_columns, size_t row_pos)
{
    prev_chunk_latest_key.clear();
    for (const auto * col : key_columns)
    {
        prev_chunk_latest_key.emplace_back(col->cloneEmpty());
        prev_chunk_latest_key.back()->insertFrom(*col, row_pos);
    }
}

bool DistinctSortedFilter::isLatestKeyFromPrevChunk(const ColumnRawPtrs & key_columns, size_t row_pos) const
{
    for (size_t i = 0, s = key_columns.size(); i < s; ++i)
    {
        const int res = prev_chunk_latest_key[i]->compareAt(0, row_pos, *key_columns[i], description[i].nulls_direction);
        if (res != 0)
            return false;
    }
    return true;
}

Chunk DistinctSortedFilter::filter(Chunk chunk, bool strip_flag)
{
    const size_t num_rows = chunk.getNumRows();
    if (unlikely(num_rows == 0))
        return chunk;

    auto columns = chunk.detachColumns();
    chassert(flag_column_pos == columns.size() - 1);

    ColumnRawPtrs key_columns;
    key_columns.reserve(key_columns_pos.size());
    for (const auto pos : key_columns_pos)
        key_columns.emplace_back(columns[pos].get());

    const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[flag_column_pos]).getData();

    IColumn::Filter filter_values(num_rows, 0);
    size_t output_rows = 0;
    size_t range_begin = 0;

    /// If the first row has the same key as the last row of the previous chunk, the previous range
    /// continues into this chunk: it was already decided at its first row, skip the continuation.
    if (!prev_chunk_latest_key.empty() && isLatestKeyFromPrevChunk(key_columns, 0))
        range_begin = getEqualRangeEndAssumeSorted(key_columns, description, 0, num_rows);

    while (range_begin != num_rows)
    {
        const size_t range_end = getEqualRangeEndAssumeSorted(key_columns, description, range_begin, num_rows);

        /// The merge of the runs must return the flagged rows before the equal unflagged ones (the
        /// sorting queues break ties by the input index and the flagged run is the input 0).
        chassert(std::is_sorted(flags.begin() + range_begin, flags.begin() + range_end, std::greater{}));

        /// Keep the first row of the range unless this value was already emitted before the spill.
        if (flags[range_begin] == 0)
        {
            filter_values[range_begin] = 1;
            ++output_rows;
        }

        range_begin = range_end;
    }

    saveLatestKey(key_columns, num_rows - 1);

    if (output_rows != num_rows)
    {
        for (auto & column : columns)
            column = column->filter(filter_values, output_rows);
    }

    if (strip_flag)
        columns.pop_back();

    return Chunk(std::move(columns), output_rows);
}

ExternalDistinctTransform::ExternalDistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    size_t max_bytes_before_external_distinct_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_,
    size_t max_block_size_rows_)
    : IProcessor({header_}, {header_})
    , distinct_set(*header_, columns_, set_size_limits_)
    , non_key_columns_rebuildable(nonKeyColumnsAreRebuildable(*header_, distinct_set.getKeyColumnsPositions()))
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , max_bytes_before_external_distinct(max_bytes_before_external_distinct_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_size_rows(max_block_size_rows_)
    , description(buildSortDescription(*header_, distinct_set.getKeyColumnsPositions()))
    , spill_header(buildSpillHeader(*header_))
    , run_dedup(distinct_set.getKeyColumnsPositions(), description, header_->columns())
    , merge_dedup(distinct_set.getKeyColumnsPositions(), description, header_->columns())
{
    chassert(max_bytes_before_external_distinct > 0);
    /// DistinctStep never uses this transform when all the distinct columns are constant.
    chassert(distinct_set.hasKeyColumns());
}

ExternalDistinctTransform::~ExternalDistinctTransform() = default;

bool ExternalDistinctTransform::firstRunFromExtraction() const
{
    return non_key_columns_rebuildable && distinct_set.supportsKeyExtraction();
}

Chunk ExternalDistinctTransform::buildChunkFromKeys(MutableColumns && key_columns) const
{
    const auto & header = inputs.front().getHeader();
    const auto & key_positions = distinct_set.getKeyColumnsPositions();
    const size_t num_rows = key_columns[0]->size();

    Columns columns(header.columns());
    for (size_t i = 0; i < key_positions.size(); ++i)
        columns[key_positions[i]] = std::move(key_columns[i]);

    for (size_t pos = 0; pos < columns.size(); ++pos)
    {
        if (!columns[pos])
            columns[pos] = header.getByPosition(pos).column->cloneResized(num_rows)->convertToFullColumnIfConst();
    }

    return Chunk(std::move(columns), num_rows);
}

Chunk ExternalDistinctTransform::prepareSpillChunk(Chunk chunk, bool already_emitted) const
{
    const size_t num_rows = chunk.getNumRows();

    /// The chunk columns follow the input header; sortBlock needs a Block to resolve the sort description.
    /// The sort must be stable: the deduplication of the merged runs keeps the first row of each range of
    /// equal keys, and the first row must stay the first-received one - both for the non-key columns of a
    /// row (when the DISTINCT key is a subset of the columns) and for the choice among values that compare
    /// equal but differ in the binary representation (0. and -0., NaN payloads).
    Block block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
    sortBlock(block, description, /*limit=*/ 0, IColumn::PermutationSortStability::Stable);

    auto columns = block.getColumns();
    columns.emplace_back(ColumnUInt8::create(num_rows, static_cast<UInt8>(already_emitted)));
    return Chunk(std::move(columns), num_rows);
}

void ExternalDistinctTransform::startFirstSpill()
{
    spilled = true;

    LOG_TRACE(log, "Switching DISTINCT to the external mode (query memory: {}, limit: {})",
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    Chunks run_chunks;
    size_t run_bytes = 0;
    auto add_run_chunk = [&](Chunk chunk)
    {
        auto prepared = prepareSpillChunk(std::move(chunk), /*already_emitted=*/ true);
        run_bytes += prepared.allocatedBytes();
        run_chunks.push_back(std::move(prepared));
    };

    if (firstRunFromExtraction())
    {
        /// The rows of the first run only suppress the equal rows during the merge and are never
        /// emitted themselves, so the keys extracted from the set (plus the constant columns rebuilt
        /// from the header) are all it needs. The set is freed right after the extraction, before the
        /// sorting: this way the transient peak is the set plus the raw keys, not plus the sorted
        /// copies.
        auto key_batches = distinct_set.extractKeyColumns(max_block_size_rows);
        distinct_set.clear();

        for (auto & key_columns : key_batches)
            add_run_chunk(buildChunkFromKeys(std::move(key_columns)));
    }
    else
    {
        /// The set is used only for lookups, and after the spill all the deduplication happens in the
        /// final merge, so the memory can be freed right away.
        distinct_set.clear();

        for (auto & chunk : emitted_buffer)
            add_run_chunk(std::move(chunk));
        emitted_buffer.clear();
    }

    startSpillRun(std::move(run_chunks), run_bytes, /*is_first_run=*/ true);
}

void ExternalDistinctTransform::startSpillRun(Chunks run_chunks, size_t run_bytes, bool is_first_run)
{
    if (!tmp_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDisk is not set for ExternalDistinctTransform");
    ++temporary_files_num;

    LOG_TRACE(log, "Will dump distinct run ({} chunks, {}) to disk (query memory: {}, limit: {})",
        run_chunks.size(),
        formatReadableSizeWithBinarySuffix(run_bytes),
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    /// If there's less free disk space than reserve_size, an exception will be thrown.
    const size_t reserve_size = run_bytes + min_free_disk_space;
    TemporaryBlockStreamHolder tmp_stream(spill_header, tmp_data, reserve_size);

    /// The limit hint cannot be applied inside the sort or the merge: rows are suppressed by the
    /// deduplication after them, so cutting the streams at `limit_hint` rows could lose distinct values.
    merge_sorter = std::make_unique<MergeSorter>(spill_header, std::move(run_chunks), description, max_block_size_rows, /*limit=*/ 0);
    current_run_is_first = is_first_run;
    if (!is_first_run)
        run_dedup.reset();

    auto sink = std::make_shared<BufferingToFileSink>(spill_header, std::move(tmp_stream), log);
    auto source = std::make_shared<BufferingFromFileSource>(spill_header, sink->getHolder(), log);

    processors.emplace_back(source);
    processors.emplace_back(sink);

    if (!external_merging_sorted)
    {
        external_merging_sorted = std::make_shared<MergingSortedTransform>(
            spill_header,
            /*num_inputs=*/ 0,
            description,
            max_block_size_rows,
            /*max_block_size_bytes=*/ 0,
            /*max_dynamic_subcolumns=*/ std::nullopt,
            SortingQueueStrategy::Batch,
            /*limit_=*/ 0,
            /*always_read_till_end_=*/ false,
            /*out_row_sources_buf_=*/ nullptr,
            /*filter_column_name_=*/ std::nullopt,
            /*use_average_block_sizes=*/ false,
            /*apply_virtual_row_conversions=*/ false,
            /*virtual_row_prefetch_window=*/ 0,
            /*have_all_inputs_=*/ false);

        processors.emplace_back(external_merging_sorted);
    }

    stage = Stage::Serialize;
    sum_bytes_in_chunks = 0;
}

IProcessor::PipelineUpdate ExternalDistinctTransform::updatePipeline()
{
    if (processors.size() > 2)
    {
        /// The first spill: add the port through which the merged stream comes back.
        inputs.emplace_back(*spill_header, this);
        connect(external_merging_sorted->getOutputs().front(), inputs.back());
    }

    auto & source = processors.front();

    static_cast<MergingSortedTransform &>(*external_merging_sorted).addInput();
    connect(source->getOutputs().back(), external_merging_sorted->getInputs().back());

    if (processors.size() > 1)
    {
        auto & sink = *std::next(processors.begin());
        /// Serialize: the run flows out through a new output port into the sink.
        outputs.emplace_back(*spill_header, this);
        connect(sink->getOutputs().front(), source->getInputs().front());
        connect(getOutputs().back(), sink->getInputs().back());
    }
    else
    {
        /// Generate: the leftover in-memory chunks were added as the last input of the merge.
        static_cast<MergingSortedTransform &>(*external_merging_sorted).setHaveAllInputs();
    }

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

IProcessor::Status ExternalDistinctTransform::prepare()
{
    if (stage == Stage::Serialize)
    {
        if (!processors.empty())
            return Status::UpdatePipeline;

        auto status = prepareSerialize();
        if (status != Status::Finished)
            return status;

        stage = Stage::Consume;
    }

    if (stage == Stage::Consume)
    {
        auto status = prepareConsume();
        if (status != Status::Finished)
            return status;

        stage = Stage::Generate;
    }

    /// stage == Stage::Generate

    if (!generated_prefix)
        return Status::Ready;

    if (!processors.empty())
        return Status::UpdatePipeline;

    return prepareGenerate();
}

IProcessor::Status ExternalDistinctTransform::prepareConsume()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// Check can output.

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

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    if (read_stopped)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
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

    /// Now consume.
    return Status::Ready;
}

IProcessor::Status ExternalDistinctTransform::prepareSerialize()
{
    auto & output = outputs.back();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (current_chunk)
        output.push(std::move(current_chunk));

    if (merge_sorter)
        return Status::Ready;

    output.finish();
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

    /// Nothing was spilled - everything was already streamed downstream during the Consume stage.
    if (temporary_files_num == 0)
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

    /// The port through which the merged stream of the spilled runs comes back.
    auto & input = inputs.back();

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    /// The deduplication of the merged chunk is real work, so it belongs to work().
    return Status::Ready;
}

void ExternalDistinctTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));

    if (stage == Stage::Serialize)
        serialize();

    if (stage == Stage::Generate)
        generate();
}

void ExternalDistinctTransform::consume(Chunk chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (!spilled)
    {
        Chunk filtered = distinct_set.filter(std::move(chunk));
        if (filtered.hasRows())
        {
            emitted_rows += filtered.getNumRows();

            /// Retain the emitted rows only when the first run cannot be rebuilt from the set at spill
            /// time (only the column pointers are copied here, but the emitted columns stay referenced).
            if (!firstRunFromExtraction())
                emitted_buffer.push_back(filtered.clone());
            generated_chunk = std::move(filtered);

            if (limit_hint && emitted_rows >= limit_hint)
            {
                read_stopped = true;
                return;
            }
        }

        /// A size limit with the 'break' overflow mode was reached: the partial chunk above is still
        /// emitted, and no further input can produce output.
        if (distinct_set.isLimitReached())
        {
            read_stopped = true;
            return;
        }

        if (distinct_set.getTotalRowCount() > 0 && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct))
            startFirstSpill();
    }
    else
    {
        removeSpecialColumnRepresentations(chunk);
        convertToFullIfConst(chunk);

        auto prepared = prepareSpillChunk(std::move(chunk), /*already_emitted=*/ false);
        sum_bytes_in_chunks += prepared.allocatedBytes();
        chunks.push_back(std::move(prepared));

        /// The floor on the run size prevents dumping every chunk as its own file when it is another
        /// operator that keeps the memory usage of the query above the threshold.
        const size_t min_bytes_in_run = std::min(max_bytes_before_external_distinct, MIN_BYTES_IN_RUN);
        if (sum_bytes_in_chunks >= min_bytes_in_run
            && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct))
        {
            auto run_chunks = std::move(chunks);
            chunks.clear();
            startSpillRun(std::move(run_chunks), sum_bytes_in_chunks, /*is_first_run=*/ false);
        }
    }
}

void ExternalDistinctTransform::serialize()
{
    /// The loop can process many blocks in one call when the deduplication filters whole blocks out
    /// (heavily duplicated runs), so check for cancellation: ending the run stream early is harmless
    /// when the pipeline is being torn down anyway.
    while (!isCancelled())
    {
        current_chunk = merge_sorter->read();
        if (!current_chunk)
            break;

        if (current_run_is_first)
            return;

        /// Local deduplication of the run. Pushing an empty chunk would end the temporary file stream
        /// prematurely, so fully filtered out chunks are skipped.
        current_chunk = run_dedup.filter(std::move(current_chunk), /*strip_flag=*/ false);
        if (current_chunk.hasRows())
            return;
    }

    merge_sorter.reset();
}

void ExternalDistinctTransform::generate()
{
    if (!generated_prefix)
    {
        generated_prefix = true;

        if (temporary_files_num > 0)
        {
            ProfileEvents::increment(ProfileEvents::ExternalDistinctMerge);
            LOG_INFO(log, "There are {} temporary distinct runs to merge", temporary_files_num);

            /// The leftover in-memory chunks are the last input of the merge. They are not locally
            /// deduplicated: the merge-phase deduplication collapses binary-equal rows within one input
            /// just as well.
            processors.emplace_back(std::make_shared<MergeSorterSource>(
                spill_header, std::move(chunks), description, max_block_size_rows, /*limit=*/ 0));

            merge_dedup.reset();
        }

        return;
    }

    if (!current_chunk)
        return;

    Chunk filtered = merge_dedup.filter(std::move(current_chunk), /*strip_flag=*/ true);
    if (!filtered.hasRows())
        return;

    emitted_rows += filtered.getNumRows();
    generated_chunk = std::move(filtered);

    /// Post-spill the hash set does not exist anymore. The rows limit stays exact: the number of the
    /// emitted rows is precisely the number of distinct values. The bytes limit restricts the in-memory
    /// state of the set, which is bounded by the spilling itself, so it has nothing left to check.
    if ((limit_hint && emitted_rows >= limit_hint)
        || !set_size_limits.check(emitted_rows, /*bytes=*/ 0, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        read_stopped = true;
}

}
