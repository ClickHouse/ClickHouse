#include <Processors/Merges/ReplacingSortedTransform.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReplacingSortedTransform::ReplacingSortedTransform(
    const Block & header, size_t num_inputs,
    SortDescription description_, const String & version_column,
    size_t max_block_size,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes)
    : IMergingTransform(num_inputs, header, header, true)
    , merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
    , description(std::move(description_))
    , out_row_sources_buf(out_row_sources_buf_)
    , source_chunks(num_inputs)
    , cursors(num_inputs)
    , chunk_allocator(num_inputs + max_row_refs)
{
    if (!version_column.empty())
        version_column_number = header.getPositionByName(version_column);
}

void ReplacingSortedTransform::initializeInputs()
{
    queue = SortingHeap<SortCursor>(cursors);
    is_queue_initialized = true;
}

void ReplacingSortedTransform::consume(Chunk chunk, size_t input_number)
{
    updateCursor(std::move(chunk), input_number);

    if (is_queue_initialized)
        queue.push(cursors[input_number]);
}

void ReplacingSortedTransform::updateCursor(Chunk chunk, size_t source_num)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);

    auto & source_chunk = source_chunks[source_num];

    if (source_chunk)
    {
        source_chunk = chunk_allocator.alloc(std::move(chunk));
        cursors[source_num].reset(source_chunk->getColumns(), {});
    }
    else
    {
        if (cursors[source_num].has_collation)
            throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

        source_chunk = chunk_allocator.alloc(std::move(chunk));
        cursors[source_num] = SortCursorImpl(source_chunk->getColumns(), description, source_num);
    }

    source_chunk->all_columns = cursors[source_num].all_columns;
    source_chunk->sort_columns = cursors[source_num].sort_columns;
}

void ReplacingSortedTransform::insertRow()
{
    if (out_row_sources_buf)
    {
        /// true flag value means "skip row"
        current_row_sources[max_pos].setSkipFlag(false);

        out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                   current_row_sources.size() * sizeof(RowSourcePart));
        current_row_sources.resize(0);
    }

    merged_data.insertRow(*selected_row.all_columns, selected_row.row_num, selected_row.owned_chunk->getNumRows());
    selected_row.clear();
}

void ReplacingSortedTransform::work()
{
    merge();
    prepareOutputChunk(merged_data);
}

void ReplacingSortedTransform::merge()
{
    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = queue.current();

        if (last_row.empty())
            setRowRef(last_row, current);

        RowRef current_row;
        setRowRef(current_row, current);

        bool key_differs = !current_row.hasEqualSortColumnsWith(last_row);

        /// if there are enough rows and the last one is calculated completely
        if (key_differs && merged_data.hasEnoughRows())
            return;

        if (key_differs)
        {
            /// Write the data for the previous primary key.
            insertRow();
            last_row.swap(current_row);
        }

        /// Initially, skip all rows. Unskip last on insert.
        size_t current_pos = current_row_sources.size();
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version_column_number == -1
            || selected_row.empty()
            || current->all_columns[version_column_number]->compareAt(
                current->pos, selected_row.row_num,
                *(*selected_row.all_columns)[version_column_number],
                /* nan_direction_hint = */ 1) >= 0)
        {
            max_pos = current_pos;
            setRowRef(selected_row, current);
        }

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            requestDataForInput(current.impl->order);
            return;
        }
    }

    /// We will write the data for the last primary key.
    if (!selected_row.empty())
        insertRow();

    is_finished = true;
}

}
