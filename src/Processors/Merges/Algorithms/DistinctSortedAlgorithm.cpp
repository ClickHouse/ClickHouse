#include <Processors/Merges/Algorithms/DistinctSortedAlgorithm.h>

#include <algorithm>

#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

namespace DB
{

DistinctSortedAlgorithm::DistinctSortedAlgorithm(
    SharedHeader header_, size_t num_inputs, SortDescription description_,
    size_t flag_column_pos_, size_t max_block_size_rows_)
    : header(std::move(header_))
    , description(std::move(description_))
    , flag_column_pos(flag_column_pos_)
    , num_key_columns(description.size() - 1)
    , max_block_size_rows(max_block_size_rows_)
    , current_inputs(num_inputs)
    , cursors(num_inputs)
    , merged_data(false, max_block_size_rows, 0, std::nullopt)
{
    DataTypes sort_types;
    for (const auto & column : description)
        sort_types.push_back(header->getByName(column.column_name).type);
    compileSortDescriptionIfNeeded(description, sort_types, /*increase_compile_attempts=*/ true);
}

void DistinctSortedAlgorithm::addInput()
{
    current_inputs.emplace_back();
    cursors.emplace_back();
}

void DistinctSortedAlgorithm::initialize(Inputs inputs)
{
    removeReplicatedFromSortingColumns(header, inputs, description);
    removeConstAndSparse(inputs);
    merged_data.initialize(*header, inputs);
    current_inputs = std::move(inputs);

    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        const auto & chunk = current_inputs[source_num].chunk;
        if (chunk.hasRows())
            cursors[source_num] = SortCursorImpl(*header, chunk.getColumns(), chunk.getNumRows(), description, source_num);
    }
    queue = SortingQueueBatch<SortCursor>(cursors);
}

void DistinctSortedAlgorithm::consume(Input & input, size_t source_num)
{
    removeReplicatedFromSortingColumns(header, input, description);
    removeConstAndSparse(input);
    current_inputs[source_num].swap(input);
    const auto & chunk = current_inputs[source_num].chunk;
    cursors[source_num].reset(chunk.getColumns(), *header, chunk.getNumRows());
    queue.push(cursors[source_num]);
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
    auto chunk = merged_data.pull();
    chunk.erase(flag_column_pos);
    consumed_rows = 0;
    return chunk;
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
        const auto & flags = assert_cast<const ColumnUInt8 &>(*current->all_columns[flag_column_pos]).getData();
        detail::RowRef current_key;
        current_key.set(current);
        current_key.num_columns = num_key_columns;
        size_t skipped_rows = batch_size;
        if (flags[first_row] == 0)
        {
            /// Ordinary chunks are internally unique, so only the first row can repeat a prior key.
            skipped_rows = !last_key.empty() && last_key.hasEqualSortColumnsWith(current_key);
        }

        /// The queue cannot pass a key while another input still has that key pending. Remembering the
        /// final processed key therefore also preserves suppression across batches and source refills.
        last_key = current_key;
        last_key.row_num = first_row + batch_size - 1;
        const bool source_exhausted = current->isLast(batch_size);
        if (source_exhausted)
            saveLastKey();

        const size_t rows_to_insert = batch_size - skipped_rows;
        if (rows_to_insert)
        {
            if (whole_chunk && batch_size == initial_batch_size && skipped_rows == 0)
                merged_data.insertChunk(std::move(current_inputs[current->order].chunk), rows_to_insert);
            else
                merged_data.insertRows(current->all_columns, first_row + skipped_rows, rows_to_insert, current->rows);
        }
        consumed_rows += batch_size;

        if (source_exhausted)
        {
            const size_t source_num = current->order;
            queue.removeTop();
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
