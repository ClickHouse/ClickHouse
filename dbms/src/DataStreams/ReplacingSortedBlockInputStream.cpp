#include <DataStreams/ReplacingSortedBlockInputStream.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{


void ReplacingSortedBlockInputStream::insertRow(ColumnPlainPtrs & merged_columns, size_t & merged_rows)
{
    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*selected_row.columns[i], selected_row.row_num);
}


Block ReplacingSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    if (children.size() == 1)
        return children[0]->read();

    Block merged_block;
    ColumnPlainPtrs merged_columns;

    init(merged_block, merged_columns);
    if (merged_columns.empty())
        return Block();

    /// Additional initialization.
    if (selected_row.empty())
    {
        selected_row.columns.resize(num_columns);

        if (!version_column.empty())
            version_column_number = merged_block.getPositionByName(version_column);
    }

    if (has_collation)
        merge(merged_columns, queue_with_collation);
    else
        merge(merged_columns, queue);

    return merged_block;
}


template<class TSortCursor>
void ReplacingSortedBlockInputStream::merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them into `merged_block` until rows no more than `max_block_size`
    while (!queue.empty())
    {
        TSortCursor current = queue.top();

        if (current_key.empty())
        {
            current_key.columns.resize(description.size());
            next_key.columns.resize(description.size());

            setPrimaryKeyRef(current_key, current);
        }

        UInt64 version = version_column_number != -1
            ? current->all_columns[version_column_number]->get64(current->pos)
            : 0;

        setPrimaryKeyRef(next_key, current);

        bool key_differs = next_key != current_key;

        /// if there are enough rows and the last one is calculated completely
        if (key_differs && merged_rows >= max_block_size)
            return;

        queue.pop();

        if (key_differs)
        {
            max_version = 0;
            /// Write the data for the previous primary key.
            insertRow(merged_columns, merged_rows);
            current_key.swap(next_key);
        }

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version >= max_version)
        {
            max_version = version;
            setRowRef(selected_row, current);
        }

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            fetchNextBlock(current, queue);
        }
    }

    /// We will write the data for the last primary key.
    insertRow(merged_columns, merged_rows);

    finished = true;
}

}
