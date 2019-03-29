#include <DataStreams/AggregatingSortedBlockInputStream.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


AggregatingSortedBlockInputStream::AggregatingSortedBlockInputStream(
    const BlockInputStreams & inputs_, const SortDescription & description_, size_t max_block_size_)
    : MergingSortedBlockInputStream(inputs_, description_, max_block_size_)
{
    /// Fill in the column numbers that need to be aggregated.
    for (size_t i = 0; i < num_columns; ++i)
    {
        ColumnWithTypeAndName & column = header.safeGetByPosition(i);

        /// We leave only states of aggregate functions.
        if (!dynamic_cast<const DataTypeAggregateFunction *>(column.type.get()) && !dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
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

        if (auto simple_aggr = dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
        {
            // simple aggregate function
            SimpleAggregateDescription desc{simple_aggr->getFunction(), i};
            columns_to_simple_aggregate.emplace_back(std::move(desc));
        }
        else
        {
            // standard aggregate function
            column_numbers_to_aggregate.push_back(i);
        }
    }
}


Block AggregatingSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    MutableColumns merged_columns;
    init(merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return Block();

    columns_to_aggregate.resize(column_numbers_to_aggregate.size());
    for (size_t i = 0, size = columns_to_aggregate.size(); i < size; ++i)
        columns_to_aggregate[i] = typeid_cast<ColumnAggregateFunction *>(merged_columns[column_numbers_to_aggregate[i]].get());

    merge(merged_columns, queue_without_collation);
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
        {
            /// Write the simple aggregation result for the previous group.
            insertSimpleAggregationResult(merged_columns);
            return;
        }

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

            /// Write the simple aggregation result for the previous group.
            if (merged_rows > 0)
                insertSimpleAggregationResult(merged_columns);

            /// Reset simple aggregation states for next row
            for (auto & desc : columns_to_simple_aggregate)
                desc.createState();

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

    /// Write the simple aggregation result for the previous group.
    insertSimpleAggregationResult(merged_columns);

    finished = true;
}


void AggregatingSortedBlockInputStream::addRow(SortCursor & cursor)
{
    for (size_t i = 0, size = column_numbers_to_aggregate.size(); i < size; ++i)
    {
        size_t j = column_numbers_to_aggregate[i];
        columns_to_aggregate[i]->insertMergeFrom(*cursor->all_columns[j], cursor->pos);
    }

    for (auto & desc : columns_to_simple_aggregate)
    {
        auto & col = cursor->all_columns[desc.column_number];
        desc.add_function(desc.function.get(), desc.state.data(), &col, cursor->pos, nullptr);
    }
}

void AggregatingSortedBlockInputStream::insertSimpleAggregationResult(MutableColumns & merged_columns)
{
    for (auto & desc : columns_to_simple_aggregate)
    {
        desc.function->insertResultInto(desc.state.data(), *merged_columns[desc.column_number]);
        desc.destroyState();
    }
}

}
