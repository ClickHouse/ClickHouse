#include <DataStreams/AggregatingSortedBlockInputStream.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


Block AggregatingSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    if (children.size() == 1)
        return children[0]->read();

    Block header;
    MutableColumns merged_columns;

    init(header, merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return Block();

    /// Additional initialization.
    if (next_key.empty())
    {
        /// Fill in the column numbers that need to be aggregated.
        for (size_t i = 0; i < num_columns; ++i)
        {
            ColumnWithTypeAndName & column = header.safeGetByPosition(i);

            /// We leave only states of aggregate functions.
            if (!startsWith(column.type->getName(), "AggregateFunction"))
            {
                column_numbers_not_to_aggregate.push_back(i);
                continue;
            }

            /// Included into PK?
            SortDescription::const_iterator it = description.begin();
            for (; it != description.end(); ++it)
                if (it->column_name == column.name || (it->column_name.empty() && it->column_number == i))
                    break;

            if (it != description.end())
            {
                column_numbers_not_to_aggregate.push_back(i);
                continue;
            }

            column_numbers_to_aggregate.push_back(i);
        }
    }

    columns_to_aggregate.resize(column_numbers_to_aggregate.size());
    for (size_t i = 0, size = columns_to_aggregate.size(); i < size; ++i)
        columns_to_aggregate[i] = typeid_cast<ColumnAggregateFunction *>(merged_columns[column_numbers_to_aggregate[i]].get());

    merge(merged_columns, queue);
    return header.cloneWithColumns(std::move(merged_columns));
}


void AggregatingSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    /// We take the rows in the correct order and put them in `merged_block`, while the rows are no more than `max_block_size`
    while (!queue.empty())
    {
        SortCursor current = queue.top();

        setPrimaryKeyRef(next_key, current);

        bool key_differs;

        if (current_key.empty())    /// The first key encountered.
        {
            setPrimaryKeyRef(current_key, current);
            key_differs = true;
        }
        else
            key_differs = next_key != current_key;

        /// if there are enough rows accumulated and the last one is calculated completely
        if (key_differs && merged_rows >= max_block_size)
            return;

        queue.pop();

        if (key_differs)
        {
            current_key.swap(next_key);

            /// We will write the data for the group. We copy the values of ordinary columns.
            for (size_t i = 0, size = column_numbers_not_to_aggregate.size(); i < size; ++i)
            {
                size_t j = column_numbers_not_to_aggregate[i];
                merged_columns[j]->insertFrom(*current->all_columns[j], current->pos);
            }

            /// Add the empty aggregation state to the aggregate columns. The state will be updated in the `addRow` function.
            for (auto & column_to_aggregate : columns_to_aggregate)
                column_to_aggregate->insertDefault();

            ++merged_rows;
        }

        addRow(current);

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }
        else
        {
            /// We fetch the next block from the appropriate source, if there is one.
            fetchNextBlock(current, queue);
        }
    }

    finished = true;
}


void AggregatingSortedBlockInputStream::addRow(SortCursor & cursor)
{
    for (size_t i = 0, size = column_numbers_to_aggregate.size(); i < size; ++i)
    {
        size_t j = column_numbers_to_aggregate[i];
        columns_to_aggregate[i]->insertMergeFrom(*cursor->all_columns[j], cursor->pos);
    }
}

}
