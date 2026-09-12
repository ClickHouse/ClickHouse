#include <Processors/Transforms/ExternalLimitByTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/SortCursor.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Processors/ISource.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/SortingTransform.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event ExternalLimitByWritePart;
    extern const Event ExternalLimitByMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Reads back one run written by `ExternalLimitByTransform`. Unlike `BufferingFromFileSource` it needs
/// no input port to wait on, because the run is fully written before the merge is built.
class TemporaryRunSource final : public ISource
{
public:
    TemporaryRunSource(SharedHeader header, TemporaryBlockStreamHolder & run_)
        : ISource(std::move(header))
        , run(run_)
    {
    }

    String getName() const override { return "ExternalLimitByRunSource"; }

    /// These rows were already counted when they were read from the original source.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

protected:
    Chunk generate() override
    {
        if (!reader)
            reader = run.getReadStream();

        Block block = reader.value()->read();
        if (block.empty())
            return {};

        const UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

private:
    TemporaryBlockStreamHolder & run;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
};

/// A name for a service column that no column of `block` already has.
String makeServiceColumnName(const Block & block, const String & wanted)
{
    String name = wanted;
    for (size_t attempt = 0; block.has(name); ++attempt)
        name = fmt::format("{}_{}", wanted, attempt);
    return name;
}

}

ExternalLimitByTransform::ExternalLimitByTransform(
    SharedHeader header,
    UInt64 group_length_,
    UInt64 group_offset_,
    const Names & column_names,
    size_t max_bytes_in_state_before_external_limit_by_,
    size_t max_bytes_in_query_before_external_limit_by_,
    size_t max_block_size_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_)
    : IProcessor({header}, {header})
    , group_offset(group_offset_)
    , group_limit_end(computeGroupLimitEnd(group_length_, group_offset_))
    , max_bytes_in_state_before_external_limit_by(max_bytes_in_state_before_external_limit_by_)
    , max_bytes_in_query_before_external_limit_by(max_bytes_in_query_before_external_limit_by_)
    , max_block_size(max_block_size_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
{
    if (!tmp_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary data storage for external LIMIT BY is not provided");

    const size_t num_columns = header->columns();
    const_columns_to_remove.assign(num_columns, false);
    for (size_t position = 0; position < num_columns; ++position)
    {
        const auto & column = header->getByPosition(position);
        if (column.column && isColumnConst(*column.column))
            const_columns_to_remove[position] = true;
        else
            header_without_constants.insert(column);
    }

    /// Constant grouping keys do not distinguish groups and are dropped along with every other constant,
    /// so the key positions are resolved against the header the transform actually works on.
    grouping_keys = filterNonConstKeys(header_without_constants, filterNonConstKeys(*header, column_names).names);

    mapping.emplace(header_without_constants, grouping_keys.names);

    Block spill_block = header_without_constants;

    const auto uint8_type = std::make_shared<DataTypeUInt8>();
    const auto uint64_type = std::make_shared<DataTypeUInt64>();

    const String is_state_name = makeServiceColumnName(spill_block, "__limit_by_is_group_state");
    spill_block.insert({uint8_type->createColumn(), uint8_type, is_state_name});
    const String rows_seen_name = makeServiceColumnName(spill_block, "__limit_by_rows_seen");
    spill_block.insert({uint64_type->createColumn(), uint64_type, rows_seen_name});
    const String arrival_name = makeServiceColumnName(spill_block, "__limit_by_arrival");
    spill_block.insert({uint64_type->createColumn(), uint64_type, arrival_name});

    is_state_column_position = spill_block.getPositionByName(is_state_name);
    rows_seen_column_position = spill_block.getPositionByName(rows_seen_name);
    arrival_column_position = spill_block.getPositionByName(arrival_name);

    /// The group state row of a key sorts before that key's data rows, which follow in input order, so
    /// that one pass over the merged stream can resume the counter before it needs it.
    run_description.reserve(grouping_keys.names.size() + 2);
    for (const auto & name : grouping_keys.names)
        run_description.emplace_back(name, 1, 1);
    run_description.emplace_back(is_state_name, -1, 1);
    run_description.emplace_back(arrival_name, 1, 1);

    spill_header = std::make_shared<const Block>(std::move(spill_block));

    previous_merged_chunk_last_key_columns.reserve(grouping_keys.positions.size());
    for (size_t position : grouping_keys.positions)
        previous_merged_chunk_last_key_columns.push_back(header_without_constants.getByPosition(position).type->createColumn());
}

ExternalLimitByTransform::~ExternalLimitByTransform() = default;

IProcessor::Status ExternalLimitByTransform::prepare()
{
    if (stage == Stage::Consume)
    {
        auto status = prepareConsume();
        if (status != Status::Finished)
            return status;

        stage = Stage::Generate;
    }

    /// Without a spill every surviving row was already emitted while consuming.
    if (!spilled)
    {
        outputs.front().finish();
        return Status::Finished;
    }

    if (!merging_pipeline_built)
    {
        buildMergingPipeline();
        merging_pipeline_built = true;
        return Status::UpdatePipeline;
    }

    return prepareGenerate();
}

IProcessor::Status ExternalLimitByTransform::prepareConsume()
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

    if (generated_chunk)
        output.push(std::move(generated_chunk));

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

IProcessor::Status ExternalLimitByTransform::prepareGenerate()
{
    auto & input = inputs.back();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();

        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    if (!merged_chunk)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        merged_chunk = input.pull();
    }

    return Status::Ready;
}

void ExternalLimitByTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));
    else
        generate();
}

void ExternalLimitByTransform::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    if (!spilled)
    {
        filterChunkInMemory(chunk);

        if (chunk.getNumRows() != 0)
        {
            if (rows_before_limit_at_least)
                rows_before_limit_at_least->add(chunk.getNumRows());
            generated_chunk = std::move(chunk);
        }

        /// The chunk above is already counted into the grouping state, so a spill starting here loses
        /// nothing: its group state rows carry those counts into the merge.
        if (shouldSpill())
            convertGroupsToStateRows();

        return;
    }

    removeConstColumns(chunk);
    bufferChunkForSpill(std::move(chunk));

    if (buffered_bytes > max_bytes_in_state_before_external_limit_by || shouldSpill())
        spillBufferedChunks();
}

void ExternalLimitByTransform::filterChunkInMemory(Chunk & chunk)
{
    /// A previous call may have thrown after populating this reused scratch buffer, so start each chunk
    /// from empty.
    output_slices.clear();

    const UInt64 row_count = chunk.getNumRows();
    auto chunk_columns = chunk.detachColumns();

    auto process_run = [&](UInt64 run_start_row, UInt64 run_row_count, UInt64 group_rows_seen_before_run)
    {
        if (group_rows_seen_before_run >= group_limit_end)
            return false;

        const auto slice
            = shrinkRunToLimitWindow(run_start_row, run_row_count, group_rows_seen_before_run, group_offset, group_limit_end);
        if (slice.length > 0)
            output_slices.push_back(slice);

        return true;
    };

    if (mapping->isTrivial())
    {
        /// Every grouping key is constant, so the whole input is one group.
        if (process_run(0, row_count, trivial_group_rows_seen))
            trivial_group_rows_seen += row_count;
    }
    else
    {
        Columns normalized_key_columns;
        normalized_key_columns.reserve(grouping_keys.positions.size());
        ColumnRawPtrs key_columns;
        key_columns.reserve(grouping_keys.positions.size());
        for (size_t position : grouping_keys.positions)
        {
            normalized_key_columns.push_back(removeSpecialRepresentations(chunk_columns[position])->convertToFullColumnIfConst());
            key_columns.push_back(normalized_key_columns.back().get());
        }

        bool cancelled = false;

        mapping->mapChunk(
            key_columns,
            row_count,
            [&](UInt64 run_start_row, UInt64 run_row_count, size_t group_idx)
            {
                const UInt64 rows_seen = mapping->getRowsSeen(group_idx);
                if (process_run(run_start_row, run_row_count, rows_seen))
                    mapping->setRowsSeen(group_idx, rows_seen + run_row_count);
            },
            [&](UInt64)
            {
                cancelled = isCancelled();
                return !cancelled;
            });

        if (cancelled)
        {
            LOG_TEST(log, "Cancelled during row processing");
            output_slices.clear();
            return;
        }
    }

    if (output_slices.empty())
        return;

    materializeSlicesIntoChunk(chunk, std::move(chunk_columns), row_count, output_slices);
}

bool ExternalLimitByTransform::shouldSpill() const
{
    /// A constant-keyed `LIMIT BY` holds one counter, and a spill can only carry over groups that exist.
    if (mapping->isTrivial() || mapping->getNumGroups() == 0)
        return false;

    if (max_bytes_in_state_before_external_limit_by && mapping->allocatedBytes() > max_bytes_in_state_before_external_limit_by)
        return true;

    return max_bytes_in_query_before_external_limit_by
        && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_in_query_before_external_limit_by);
}

void ExternalLimitByTransform::convertGroupsToStateRows()
{
    const size_t num_groups = mapping->getNumGroups();
    const size_t num_data_columns = header_without_constants.columns();

    MutableColumns key_columns;
    key_columns.reserve(grouping_keys.positions.size());
    for (size_t position : grouping_keys.positions)
        key_columns.push_back(header_without_constants.getByPosition(position).type->createColumn());

    PaddedPODArray<UInt64> rows_seen;
    mapping->extractGroups(key_columns, rows_seen);

    LOG_DEBUG(
        log,
        "Spilling LIMIT BY: writing {} groups holding {} out as group state rows",
        num_groups,
        formatReadableSizeWithBinarySuffix(mapping->allocatedBytes()));

    mapping.reset();
    spilled = true;

    if (rows_seen.empty())
        return;

    const UInt64 num_rows = rows_seen.size();

    /// A group state row carries only the key; the other data columns are never read for it.
    MutableColumns columns(num_data_columns);
    for (size_t position = 0; position < num_data_columns; ++position)
        columns[position] = header_without_constants.getByPosition(position).type->createColumn();

    for (size_t key_idx = 0; key_idx < grouping_keys.positions.size(); ++key_idx)
        columns[grouping_keys.positions[key_idx]] = std::move(key_columns[key_idx]);

    for (size_t position = 0; position < num_data_columns; ++position)
    {
        if (columns[position]->size() != num_rows)
            columns[position]->insertManyDefaults(num_rows - columns[position]->size());
    }

    auto is_state_column = ColumnUInt8::create(num_rows, UInt8(1));
    auto rows_seen_column = ColumnUInt64::create();
    rows_seen_column->getData() = std::move(rows_seen);
    auto arrival_column = ColumnUInt64::create(num_rows, 0);

    Columns spill_columns;
    spill_columns.reserve(spill_header->columns());
    for (auto & column : columns)
        spill_columns.push_back(std::move(column));
    spill_columns.push_back(std::move(is_state_column));
    spill_columns.push_back(std::move(rows_seen_column));
    spill_columns.push_back(std::move(arrival_column));

    Chunk state_chunk(std::move(spill_columns), num_rows);
    addSortedChunkToBuffer(std::move(state_chunk));

    if (buffered_bytes > max_bytes_in_state_before_external_limit_by)
        spillBufferedChunks();
}

void ExternalLimitByTransform::bufferChunkForSpill(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    auto columns = chunk.detachColumns();

    /// Runs go through the Native format and the sorting cursors, which want plain columns.
    for (auto & column : columns)
        column = removeSpecialRepresentations(column->convertToFullColumnIfConst());

    auto arrival_column = ColumnUInt64::create();
    auto & arrival_data = arrival_column->getData();
    arrival_data.resize(num_rows);
    for (UInt64 row = 0; row < num_rows; ++row)
        arrival_data[row] = arrival_counter + row;
    arrival_counter += num_rows;

    columns.push_back(ColumnUInt8::create(num_rows, UInt8(0)));
    columns.push_back(ColumnUInt64::create(num_rows, 0));
    columns.push_back(std::move(arrival_column));

    addSortedChunkToBuffer(Chunk(std::move(columns), num_rows));
}

void ExternalLimitByTransform::addSortedChunkToBuffer(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();

    /// The merge takes cursors over the buffered chunks, so each of them has to be sorted on its own.
    Block block = spill_header->cloneWithColumns(chunk.detachColumns());
    sortBlock(block, run_description);

    Chunk sorted_chunk(block.getColumns(), num_rows);
    buffered_bytes += sorted_chunk.allocatedBytes();
    buffered_chunks.push_back(std::move(sorted_chunk));
}

void ExternalLimitByTransform::spillBufferedChunks()
{
    if (buffered_chunks.empty())
        return;

    const size_t reserve_size = buffered_bytes + min_free_disk_space;
    TemporaryBlockStreamHolder run(spill_header, tmp_data, reserve_size);

    MergeSorter sorter(spill_header, std::move(buffered_chunks), run_description, max_block_size, 0);
    while (auto chunk = sorter.read())
        run->write(spill_header->cloneWithColumns(chunk.detachColumns()));

    auto stat = run.finishWriting();
    LOG_DEBUG(
        log,
        "Wrote a LIMIT BY run into {}, compressed {}, uncompressed {}",
        run.getHolder()->describeFilePath(),
        ReadableSize(static_cast<double>(stat.compressed_size)),
        ReadableSize(static_cast<double>(stat.uncompressed_size)));

    runs.push_back(std::move(run));
    buffered_chunks.clear();
    buffered_bytes = 0;

    ProfileEvents::increment(ProfileEvents::ExternalLimitByWritePart);
}

void ExternalLimitByTransform::buildMergingPipeline()
{
    const size_t num_inputs = runs.size() + (buffered_chunks.empty() ? 0 : 1);
    if (num_inputs == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "External LIMIT BY has spilled but has nothing to merge");

    ProfileEvents::increment(ProfileEvents::ExternalLimitByMerge);
    LOG_DEBUG(log, "Merging {} temporary LIMIT BY runs and {} buffered chunks", runs.size(), buffered_chunks.size());

    auto merging = std::make_shared<MergingSortedTransform>(
        spill_header,
        num_inputs,
        run_description,
        max_block_size,
        /*max_block_size_bytes=*/0,
        /*max_dynamic_subcolumns=*/std::nullopt,
        SortingQueueStrategy::Batch);

    auto merging_input = merging->getInputs().begin();
    for (auto & run : runs)
    {
        auto source = std::make_shared<TemporaryRunSource>(spill_header, run);
        connect(source->getOutputs().front(), *merging_input);
        ++merging_input;
        merging_processors.push_back(std::move(source));
    }

    if (!buffered_chunks.empty())
    {
        auto source
            = std::make_shared<MergeSorterSource>(spill_header, std::move(buffered_chunks), run_description, max_block_size, 0);
        connect(source->getOutputs().front(), *merging_input);
        merging_processors.push_back(std::move(source));
        buffered_chunks.clear();
        buffered_bytes = 0;
    }

    merging_transform = merging;
    merging_processors.push_back(std::move(merging));
}

IProcessor::PipelineUpdate ExternalLimitByTransform::updatePipeline()
{
    inputs.emplace_back(*spill_header, this);
    connect(merging_transform->getOutputs().front(), inputs.back());

    return PipelineUpdate{.to_add = std::move(merging_processors), .to_remove = {}};
}

void ExternalLimitByTransform::generate()
{
    filterMergedChunk(merged_chunk);

    Chunk chunk = std::move(merged_chunk);
    merged_chunk = {};

    if (chunk.getNumRows() == 0)
        return;

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(chunk.getNumRows());

    enrichChunkWithConstants(chunk);
    generated_chunk = std::move(chunk);
}

void ExternalLimitByTransform::filterMergedChunk(Chunk & chunk)
{
    output_slices.clear();

    const UInt64 row_count = chunk.getNumRows();
    if (row_count == 0)
    {
        chunk.clear();
        return;
    }

    auto columns = chunk.detachColumns();

    Columns key_columns;
    key_columns.reserve(grouping_keys.positions.size());
    for (size_t position : grouping_keys.positions)
        key_columns.push_back(columns[position]);

    const auto & is_state = assert_cast<const ColumnUInt8 &>(*columns[is_state_column_position]).getData();
    const auto & rows_seen = assert_cast<const ColumnUInt64 &>(*columns[rows_seen_column_position]).getData();

    /// Row 0 can continue the group the previous merged chunk ended on. The holder is empty before the
    /// first chunk, in which case there is no previous key to compare against.
    if (!previous_merged_chunk_last_key_columns.empty() && !previous_merged_chunk_last_key_columns.front()->empty())
    {
        for (size_t key_idx = 0; key_idx < key_columns.size(); ++key_idx)
        {
            if (key_columns[key_idx]->compareAt(0, 0, *previous_merged_chunk_last_key_columns[key_idx], 1) != 0)
            {
                merged_group_rows_seen = 0;
                break;
            }
        }
    }

    UInt64 run_start_row = 0;
    while (run_start_row < row_count)
    {
        const UInt64 run_end
            = key_columns.empty() ? row_count : getEqualRangeEndAssumeSorted(key_columns, run_start_row, row_count, 1);

        /// The group's state row sorts first and is not a result row: it only resumes the counter.
        UInt64 data_start_row = run_start_row;
        if (is_state[run_start_row])
        {
            merged_group_rows_seen = rows_seen[run_start_row];
            ++data_start_row;
        }

        if (data_start_row < run_end && merged_group_rows_seen < group_limit_end)
        {
            const UInt64 run_row_count = run_end - data_start_row;
            const auto slice
                = shrinkRunToLimitWindow(data_start_row, run_row_count, merged_group_rows_seen, group_offset, group_limit_end);
            if (slice.length > 0)
                output_slices.push_back(slice);

            merged_group_rows_seen += run_row_count;
        }

        /// A group boundary inside the chunk resets the counter before the next run.
        if (run_end != row_count)
            merged_group_rows_seen = 0;

        run_start_row = run_end;
    }

    for (size_t key_idx = 0; key_idx < key_columns.size(); ++key_idx)
    {
        auto & last_key_column = previous_merged_chunk_last_key_columns[key_idx];
        if (!last_key_column->empty())
            last_key_column->popBack(1);
        last_key_column->insertFrom(*key_columns[key_idx], row_count - 1);
    }

    if (output_slices.empty())
    {
        chunk.clear();
        return;
    }

    /// The service columns must not reach the output; they sit after the data columns in `spill_header`.
    key_columns.clear();
    columns.resize(header_without_constants.columns());

    materializeSlicesIntoChunk(chunk, std::move(columns), row_count, output_slices);
}

void ExternalLimitByTransform::removeConstColumns(Chunk & chunk) const
{
    const size_t num_columns = chunk.getNumColumns();
    const size_t num_rows = chunk.getNumRows();

    if (num_columns != const_columns_to_remove.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Chunk has a different number of columns than the header: {} vs {}",
            num_columns,
            const_columns_to_remove.size());

    auto columns = chunk.detachColumns();
    Columns columns_without_constants;
    columns_without_constants.reserve(header_without_constants.columns());

    for (size_t position = 0; position < num_columns; ++position)
    {
        if (!const_columns_to_remove[position])
            columns_without_constants.push_back(std::move(columns[position]));
    }

    chunk.setColumns(std::move(columns_without_constants), num_rows);
}

void ExternalLimitByTransform::enrichChunkWithConstants(Chunk & chunk) const
{
    const size_t num_rows = chunk.getNumRows();
    const size_t num_result_columns = const_columns_to_remove.size();

    auto columns = chunk.detachColumns();
    Columns columns_with_constants;
    columns_with_constants.reserve(num_result_columns);

    const auto & header = inputs.front().getHeader();

    size_t next_non_const_column = 0;
    for (size_t position = 0; position < num_result_columns; ++position)
    {
        if (const_columns_to_remove[position])
            columns_with_constants.emplace_back(header.getByPosition(position).column->cloneResized(num_rows));
        else
        {
            if (next_non_const_column >= columns.size())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Cannot enrich the chunk with constants because it ran out of non-constant columns");

            columns_with_constants.emplace_back(std::move(columns[next_non_const_column]));
            ++next_non_const_column;
        }
    }

    chunk.setColumns(std::move(columns_with_constants), num_rows);
}

}
