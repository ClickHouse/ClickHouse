#include <Processors/Merges/Algorithms/GroupLimitSortedAlgorithm.h>

#include <algorithm>
#include <limits>

#include <base/arithmeticOverflow.h>

namespace DB
{

GroupLimitSortedAlgorithm::GroupLimitSortedAlgorithm(
    SharedHeaders input_headers,
    SharedHeader output_header_,
    SortDescription key_description,
    const std::optional<NameAndTypePair> & seen_rows_column,
    UInt64 group_offset_,
    UInt64 group_limit_end_,
    size_t max_block_size_rows_)
    : output_header(std::move(output_header_))
    , seen_column(seen_rows_column)
    , description(inputSortDescription(
          std::move(key_description), seen_column ? std::optional<String>(seen_column->name) : std::nullopt))
    , num_key_columns(description.size() - (seen_column ? 1 : 0))
    , group_offset(group_offset_)
    , group_limit_end(group_limit_end_)
    , max_block_size_rows(max_block_size_rows_)
    , merged_data(false, max_block_size_rows, 0, std::nullopt)
{
    chassert(num_key_columns > 0);
    for (auto & header : input_headers)
        addInput(std::move(header));
}

SortDescription GroupLimitSortedAlgorithm::inputSortDescription(
    SortDescription key_description, const std::optional<String> & seen_rows_column)
{
    if (seen_rows_column)
        key_description.emplace_back(*seen_rows_column, -1, 1);
    return key_description;
}

void GroupLimitSortedAlgorithm::addInput(SharedHeader header)
{
    const bool adds_zero_seen_rows = seen_column && !header->has(seen_column->name);
    if (adds_zero_seen_rows)
    {
        Block extended = *header;
        extended.insert({seen_column->type->createColumn(), seen_column->type, seen_column->name});
        header = std::make_shared<const Block>(std::move(extended));
    }
    sources.push_back({std::move(header), {}, {}, adds_zero_seen_rows});
    current_inputs.emplace_back();
    cursors.emplace_back();
}

void GroupLimitSortedAlgorithm::addZeroSeenRows(Chunk & chunk) const
{
    auto column = seen_column->type->createColumn();
    column->insertManyDefaults(chunk.getNumRows());
    chunk.addColumn(std::move(column));
}

void GroupLimitSortedAlgorithm::initialize(Inputs inputs)
{
    chassert(inputs.size() == sources.size());
    DataTypes sort_types;
    for (const auto & column : description)
    {
        chassert(!column.collator);
        sort_types.push_back(sources.front().header->getByName(column.column_name).type);
    }
    compileSortDescriptionIfNeeded(description, sort_types, /*increase_compile_attempts=*/ true);

    current_inputs = std::move(inputs);
    Inputs output_inputs(current_inputs.size());
    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        auto & source = sources[source_num];
        /// An input that is already exhausted has a chunk without columns.
        if (source.adds_zero_seen_rows && current_inputs[source_num].chunk.getNumColumns())
            addZeroSeenRows(current_inputs[source_num].chunk);
        removeReplicatedFromSortingColumns(source.header, current_inputs[source_num], description);
        removeConstAndSparse(current_inputs[source_num]);
        for (size_t i = 0; i < description.size(); ++i)
            chassert(source.header->getByName(description[i].column_name).type->equals(*sort_types[i]));
        const auto & chunk = current_inputs[source_num].chunk;
        if (chunk.hasRows())
        {
            auto & cursor = cursors[source_num];
            cursor = SortCursorImpl(*source.header, chunk.getColumns(), chunk.getNumRows(), description, source_num);
            if (seenRows(cursor, 0) == 0)
            {
                for (const auto & column : *output_header)
                {
                    const auto pos = source.header->getPositionByName(column.name);
                    chassert(source.header->getByPosition(pos).type->equals(*column.type));
                    source.output_positions.push_back(pos);
                    source.output_columns.push_back(cursor.all_columns[pos]);
                }
                output_inputs[source_num].chunk = projectOutput(chunk.clone(), source_num);
            }
        }
    }
    merged_data.initialize(*output_header, output_inputs);
    queue = SortingQueueBatch<SortCursor>(cursors);
}

void GroupLimitSortedAlgorithm::consume(Input & input, size_t source_num)
{
    auto & source = sources[source_num];
    if (source.adds_zero_seen_rows)
        addZeroSeenRows(input.chunk);
    removeReplicatedFromSortingColumns(source.header, input, description);
    removeConstAndSparse(input);
    current_inputs[source_num].swap(input);
    const auto & chunk = current_inputs[source_num].chunk;
    auto & cursor = cursors[source_num];
    cursor.reset(chunk.getColumns(), *source.header, chunk.getNumRows());
    chassert((seenRows(cursor, 0) != 0) == source.output_positions.empty());
    for (size_t i = 0; i < source.output_positions.size(); ++i)
        source.output_columns[i] = cursor.all_columns[source.output_positions[i]];
    queue.push(cursor);
}

Chunk GroupLimitSortedAlgorithm::projectOutput(Chunk chunk, size_t source_num) const
{
    const auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    Columns result;
    result.reserve(output_header->columns());
    for (const auto pos : sources[source_num].output_positions)
        result.push_back(columns[pos]);
    chunk.setColumns(std::move(result), num_rows);
    return chunk;
}

void GroupLimitSortedAlgorithm::saveLastKey()
{
    last_key_columns.clear();
    last_key_column_ptrs.clear();
    for (size_t i = 0; i < num_key_columns; ++i)
    {
        auto column = last_key.sort_columns[i]->cloneEmpty();
        column->insertFrom(*last_key.sort_columns[i], last_key.row_num);
        last_key_column_ptrs.push_back(column.get());
        last_key_columns.push_back(std::move(column));
    }
    last_key.sort_columns = last_key_column_ptrs.data();
    last_key.row_num = 0;
}

Chunk GroupLimitSortedAlgorithm::pull()
{
    consumed_rows = 0;
    return merged_data.pull();
}

UInt64 GroupLimitSortedAlgorithm::seenRows(const SortCursorImpl & cursor, size_t row) const
{
    return seen_column ? cursor.sort_columns[num_key_columns]->getUInt(row) : 0;
}

IMergingAlgorithm::Status GroupLimitSortedAlgorithm::merge()
{
    if (group_offset == 0 && group_limit_end == 1)
        return mergeDistinctRows();
    return mergeRankedRows();
}

IMergingAlgorithm::Status GroupLimitSortedAlgorithm::mergeDistinctRows()
{
    while (queue.isValid())
    {
        if (consumed_rows == max_block_size_rows)
            return Status(pull());

        auto [current_ptr, initial_batch_size] = queue.current();
        auto current = *current_ptr;
        const bool whole_chunk = current->isFirst() && current->isLast(initial_batch_size);

        /// Whole-chunk forwarding starts a new merge block, including when earlier rows were suppressed.
        if (whole_chunk && consumed_rows)
            return Status(pull());

        const size_t batch_size = std::min(initial_batch_size, max_block_size_rows - consumed_rows);
        const size_t first_row = current->getRow();
        detail::RowRef current_key;
        current_key.set(current);
        current_key.num_columns = num_key_columns;
        size_t skipped_rows = batch_size;
        if (seenRows(*current.impl, first_row) == 0)
        {
            /// Input-row chunks are internally unique, so only the first row can repeat a prior key,
            /// and only when switching chunks. Source refills compare against the saved boundary key.
            skipped_rows = !last_key.empty() && last_key.sort_columns != current_key.sort_columns
                && last_key.hasEqualSortColumnsWith(current_key);
        }

        /// The queue cannot pass a key while another input still has that key pending. Remembering the
        /// final processed key therefore also preserves suppression across batches and source refills.
        last_key = current_key;
        last_key.row_num = first_row + batch_size - 1;
        const bool source_exhausted = current->isLast(batch_size);
        if (source_exhausted)
        {
            saveLastKey();
            queue.removeTop();
        }

        const size_t rows_to_insert = batch_size - skipped_rows;
        if (rows_to_insert)
        {
            if (whole_chunk && batch_size == initial_batch_size && skipped_rows == 0)
            {
                auto & chunk = current_inputs[current->order].chunk;
                merged_data.insertChunk(projectOutput(std::move(chunk), current->order), rows_to_insert);
            }
            else
                merged_data.insertRows(sources[current->order].output_columns, first_row + skipped_rows, rows_to_insert, current->rows);
        }
        consumed_rows += batch_size;

        if (source_exhausted)
        {
            const size_t source_num = current->order;
            Status status(source_num);
            if (whole_chunk || consumed_rows == max_block_size_rows)
                status.chunk = pull();
            return status;
        }
        queue.next(batch_size);
    }

    return Status(pull(), true);
}

IMergingAlgorithm::Status GroupLimitSortedAlgorithm::mergeRankedRows()
{
    while (queue.isValid())
    {
        if (consumed_rows == max_block_size_rows)
            return Status(pull());

        auto [current_ptr, initial_batch_size] = queue.current();
        auto current = *current_ptr;
        const size_t batch_size = std::min(initial_batch_size, max_block_size_rows - consumed_rows);
        const size_t first_row = current->getRow();
        const bool is_seen_source = seenRows(*current.impl, first_row) != 0;
        const IColumn ** key_columns = current->sort_columns.data();

        size_t range_start = first_row;
        size_t range_length = 0;
        auto flush_range = [&]
        {
            if (range_length == 0)
                return;
            merged_data.insertRows(sources[current->order].output_columns, range_start, range_length, current->rows);
            range_length = 0;
        };

        for (size_t row = first_row; row < first_row + batch_size; ++row)
        {
            /// Equal keys are adjacent in the merged order, so a row continues the last key exactly when it equals
            /// the previously processed row.
            const bool same_key = row == first_row
                ? !last_key.empty() && detail::RowRef::checkEquals(num_key_columns, last_key.sort_columns, last_key.row_num, key_columns, row)
                : detail::RowRef::checkEquals(num_key_columns, key_columns, row - 1, key_columns, row);
            if (!same_key)
                last_key_rank = 0;

            if (is_seen_source)
            {
                /// A rank at or past the window end suppresses the rest of the key, so it can saturate.
                if (common::addOverflow(last_key_rank, seenRows(*current.impl, row), last_key_rank))
                    last_key_rank = std::numeric_limits<UInt64>::max();
                continue;
            }

            if (last_key_rank >= group_offset && last_key_rank < group_limit_end)
            {
                if (range_length == 0)
                    range_start = row;
                ++range_length;
            }
            else
                flush_range();

            if (last_key_rank != std::numeric_limits<UInt64>::max())
                ++last_key_rank;
        }
        flush_range();

        last_key.set(current);
        last_key.num_columns = num_key_columns;
        last_key.row_num = first_row + batch_size - 1;
        const bool source_exhausted = current->isLast(batch_size);
        if (source_exhausted)
        {
            saveLastKey();
            queue.removeTop();
        }
        consumed_rows += batch_size;

        if (source_exhausted)
        {
            Status status(current->order);
            if (consumed_rows == max_block_size_rows)
                status.chunk = pull();
            return status;
        }
        queue.next(batch_size);
    }

    return Status(pull(), true);
}

}
