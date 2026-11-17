#include <Processors/Merges/Algorithms/DistinctSortedAlgorithm.h>

#include <algorithm>

#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

namespace DB
{

DistinctSortedAlgorithm::DistinctSortedAlgorithm(
    SharedHeaders input_headers, SharedHeader output_header_, SortDescription description_, size_t max_block_size_rows_)
    : output_header(std::move(output_header_))
    , description(std::move(description_))
    , num_key_columns(description.size() - 1)
    , max_block_size_rows(max_block_size_rows_)
    , merged_data(false, max_block_size_rows, 0, std::nullopt)
{
    chassert(description.size() >= 2);
    chassert(description.back().direction == -1);
    for (auto & header : input_headers)
        addInput(std::move(header));
}

void DistinctSortedAlgorithm::addInput(SharedHeader header)
{
    sources.push_back({std::move(header), {}, {}});
    current_inputs.emplace_back();
    cursors.emplace_back();
}

void DistinctSortedAlgorithm::initialize(Inputs inputs)
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
        removeReplicatedFromSortingColumns(source.header, current_inputs[source_num], description);
        removeConstAndSparse(current_inputs[source_num]);
        for (size_t i = 0; i < description.size(); ++i)
            chassert(source.header->getByName(description[i].column_name).type->equals(*sort_types[i]));
        const auto & chunk = current_inputs[source_num].chunk;
        if (chunk.hasRows())
        {
            auto & cursor = cursors[source_num];
            cursor = SortCursorImpl(*source.header, chunk.getColumns(), chunk.getNumRows(), description, source_num);
            if (cursor.sort_columns[num_key_columns]->getUInt(0) == 0)
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

void DistinctSortedAlgorithm::consume(Input & input, size_t source_num)
{
    auto & source = sources[source_num];
    removeReplicatedFromSortingColumns(source.header, input, description);
    removeConstAndSparse(input);
    current_inputs[source_num].swap(input);
    const auto & chunk = current_inputs[source_num].chunk;
    auto & cursor = cursors[source_num];
    cursor.reset(chunk.getColumns(), *source.header, chunk.getNumRows());
    chassert((cursor.sort_columns[num_key_columns]->getUInt(0) == 0) == !source.output_positions.empty());
    for (size_t i = 0; i < source.output_positions.size(); ++i)
        source.output_columns[i] = cursor.all_columns[source.output_positions[i]];
    queue.push(cursor);
}

Chunk DistinctSortedAlgorithm::projectOutput(Chunk chunk, size_t source_num) const
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

void DistinctSortedAlgorithm::saveLastKey()
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

Chunk DistinctSortedAlgorithm::pull()
{
    consumed_rows = 0;
    return merged_data.pull();
}

IMergingAlgorithm::Status DistinctSortedAlgorithm::merge()
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
        const auto & flags = assert_cast<const ColumnUInt8 &>(*current->sort_columns[num_key_columns]).getData();
        detail::RowRef current_key;
        current_key.set(current);
        current_key.num_columns = num_key_columns;
        size_t skipped_rows = batch_size;
        if (flags[first_row] == 0)
        {
            /// Ordinary chunks are internally unique, so only the first row can repeat a prior key,
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

}
