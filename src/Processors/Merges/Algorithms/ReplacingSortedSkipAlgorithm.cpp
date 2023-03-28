#include <Processors/Merges/Algorithms/ReplacingSortedSkipAlgorithm.h>
#include <IO/WriteBuffer.h>

namespace DB
{

ReplacingSortedSkipAlgorithm::ReplacingSortedSkipAlgorithm(
    const Block & header_,
    const Block & output_header,
    size_t num_inputs,
    SortDescription description_,
    const String & version_column,
    size_t max_block_size,
    bool use_average_block_sizes)

    :
      cursors(num_inputs)
    , header(std::move(header_))
    , description(std::move(description_))
    , chunk_allocator(num_inputs + max_row_refs)
    , sources(num_inputs)
    , merged_data(output_header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
{
    if (!version_column.empty())
        version_column_number = header_.getPositionByName(version_column);
}


void ReplacingSortedSkipAlgorithm::initialize(Inputs inputs)
{
    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        if (!inputs[source_num].chunk)
            continue;

        auto & source = sources[source_num];

        MutableColumnPtr column = ColumnUInt8::create();
        column->reserve(inputs[source_num].chunk.getNumRows());

        source.skip_last_row = inputs[source_num].skip_last_row;
        source.skip_chunk = false;
        source.chunk = chunk_allocator.alloc(inputs[source_num].chunk);
        source.chunk->filter_column = std::move(column);
        cursors[source_num] = SortCursorImpl(header, source.chunk->getColumns(), description, source_num, inputs[source_num].permutation);

        source.chunk->all_columns = cursors[source_num].all_columns;
        source.chunk->sort_columns = cursors[source_num].sort_columns;
    }

    queue = SortingQueue<SortCursor>(cursors);
}

void ReplacingSortedSkipAlgorithm::consume(Input & input, size_t source_num)
{

    auto & source = sources[source_num];
    source.skip_last_row = input.skip_last_row;
    source.chunk = chunk_allocator.alloc(input.chunk);
    cursors[source_num].reset(source.chunk->getColumns(), header, input.permutation);

    source.chunk->all_columns = cursors[source_num].all_columns;
    source.chunk->sort_columns = cursors[source_num].sort_columns;

    queue.push(cursors[source_num]);

    // If the chunk last row is below all other chunks position we can pass the chunk directly
    bool skip = true;

    if (cursors[source_num].isValid() && sources[source_num].chunk->getNumRows() > 0)
    {

        detail::RowRef last_row;
        last_row.setLast(cursors[source_num]);

        for (size_t source_idx = 0; source_idx < sources.size(); ++source_idx)
        {
            if (source_idx == source_num || !sources[source_idx].chunk || !cursors[source_idx].isValid())
                continue;

            detail::RowRef current_row;
            current_row.setCur(cursors[source_num]);

            skip = last_row.hasLowerSortColumnsWith(current_row);

            if (!skip)
                break;
        }
    }
    source.skip_chunk = skip;

    if (!skip)
    {
        MutableColumnPtr column = ColumnUInt8::create();

        column->reserve(source.chunk->getNumRows());
        source.chunk->filter_column = std::move(column);
    }
}


void ReplacingSortedSkipAlgorithm::emitChunk(detail::SharedChunkPtr &chunk, bool skip)
{
    if (!selected_row.empty() && chunk == selected_row.owned_chunk)
        return;

    if (skip || !chunk->filter_column)
    {
        chunk->addColumn(ColumnConst::create(ColumnUInt8::create(1, 1), chunk->getNumRows()));
    }
    else
    {
        chunk->addColumn(std::move(chunk->filter_column));
    }
    size_t rows = chunk->getNumRows();
    merged_data.insertChunkPtr(chunk, rows);
}

IMergingAlgorithm::Status ReplacingSortedSkipAlgorithm::merge()
{
    if (merged_data.hasEnoughRows())
        return Status(merged_data.pull());

    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = queue.current();

        if (sources[current->order].skip_chunk || (current->isLast() && skipLastRowFor(current->order)))
        {
            bool skip = sources[current->order].skip_chunk;
            if (!skip)
                sources[current.impl->order].chunk->filter_column->insert(0);

            emitChunk(sources[current->order].chunk, skip);
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        RowRef current_row;
        setRowRef(current_row, current);

        bool key_differs = selected_row.empty() || !current_row.hasEqualSortColumnsWith(selected_row);
        if (key_differs)
        {
            /// if there are enough rows and the last one is calculated completely
            if (merged_data.hasEnoughRows())
                return Status(merged_data.pull());

            /// Write the data for the previous primary key.
            if (!selected_row.empty())
            {
                selected_row.owned_chunk->filter_column->insert(1);
                if (selected_row.row_num + 1 >= selected_row.owned_chunk->getNumRows())
                {
                    detail::SharedChunkPtr chunk = std::move(selected_row.owned_chunk);
                    selected_row.clear();
                    emitChunk(chunk, false);
                    return Status(merged_data.pull());
                }
            }

            selected_row.clear();
        }

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version_column_number == -1
            || selected_row.empty()
            || current->all_columns[version_column_number]->compareAt(
                current->getRow(), selected_row.row_num,
                *(*selected_row.all_columns)[version_column_number],
                /* nan_direction_hint = */ 1) >= 0)
        {
            if (!selected_row.empty())
            {
                selected_row.owned_chunk->filter_column->insert(0);
                if (selected_row.row_num + 1 >= selected_row.owned_chunk->getNumRows())
                {
                    detail::SharedChunkPtr chunk = std::move(selected_row.owned_chunk);
                    selected_row.clear();
                    emitChunk(chunk, false);
                    return Status(merged_data.pull());
                }
            }

            setRowRef(selected_row, current);
        }
        else
        {
            current_row.owned_chunk->filter_column->insert(0);
        }

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            emitChunk(sources[current.impl->order].chunk, false);
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    /// If have enough rows, return block, because it prohibited to overflow requested number of rows.
    if (merged_data.hasEnoughRows())
        return Status(merged_data.pull());

    /// We will write the data for the last primary key.
    if (!selected_row.empty())
    {
        selected_row.owned_chunk->filter_column->insert(1);
        if (selected_row.row_num + 1 >= selected_row.owned_chunk->getNumColumns())
        {
            detail::SharedChunkPtr chunk = std::move(selected_row.owned_chunk);
            selected_row.clear();
            emitChunk(chunk, false);
        }
    }

    return Status(merged_data.pull(), true);
}

}
