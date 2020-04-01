#include <DataStreams/AggregatingSortedBlockInputStream.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Arena.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class RemovingLowCardinalityBlockInputStream : public IBlockInputStream
{
public:
    RemovingLowCardinalityBlockInputStream(BlockInputStreamPtr input_, ColumnNumbers positions_)
        : input(std::move(input_)), positions(std::move(positions_))
    {
        header = transform(input->getHeader());
    }

    Block transform(Block block)
    {
        if (block)
        {
            for (auto & pos : positions)
            {
                auto & col = block.safeGetByPosition(pos);
                col.column = recursiveRemoveLowCardinality(col.column);
                col.type = recursiveRemoveLowCardinality(col.type);
            }
        }

        return block;
    }

    String getName() const override { return "RemovingLowCardinality"; }
    Block getHeader() const override { return header; }
    const BlockMissingValues & getMissingValues() const override { return input->getMissingValues(); }
    bool isSortedOutput() const override { return input->isSortedOutput(); }
    const SortDescription & getSortDescription() const override { return input->getSortDescription(); }

protected:
    Block readImpl() override { return transform(input->read()); }

private:
    Block header;
    BlockInputStreamPtr input;
    ColumnNumbers positions;
};


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
            if (desc.function->allocatesMemoryInArena())
                allocatesMemoryInArena = true;

            columns_to_simple_aggregate.emplace_back(std::move(desc));

            if (recursiveRemoveLowCardinality(column.type).get() != column.type.get())
                converted_lc_columns.emplace_back(i);
        }
        else
        {
            // standard aggregate function
            column_numbers_to_aggregate.push_back(i);
        }
    }

    result_header = header;

    if (!converted_lc_columns.empty())
    {
        for (auto & input : children)
            input = std::make_shared<RemovingLowCardinalityBlockInputStream>(input, converted_lc_columns);

        header = children.at(0)->getHeader();
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

    for (auto & pos : converted_lc_columns)
    {
        auto & from_type = header.getByPosition(pos).type;
        auto & to_type = result_header.getByPosition(pos).type;
        merged_columns[pos] = (*recursiveTypeConversion(std::move(merged_columns[pos]), from_type, to_type)).mutate();
    }

    return result_header.cloneWithColumns(std::move(merged_columns));
}


void AggregatingSortedBlockInputStream::merge(MutableColumns & merged_columns, SortingHeap<SortCursor> & queue)
{
    size_t merged_rows = 0;

    /// We take the rows in the correct order and put them in `merged_block`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = queue.current();

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

        if (key_differs)
        {
            current_key.swap(next_key);

            /// We will write the data for the group. We copy the values of ordinary columns.
            for (size_t j : column_numbers_not_to_aggregate)
                merged_columns[j]->insertFrom(*current->all_columns[j], current->pos);

            /// Add the empty aggregation state to the aggregate columns. The state will be updated in the `addRow` function.
            for (auto & column_to_aggregate : columns_to_aggregate)
                column_to_aggregate->insertDefault();

            /// Write the simple aggregation result for the previous group.
            if (merged_rows > 0)
                insertSimpleAggregationResult(merged_columns);

            /// Reset simple aggregation states for next row
            for (auto & desc : columns_to_simple_aggregate)
                desc.createState();

            if (allocatesMemoryInArena)
                arena = std::make_unique<Arena>();

            ++merged_rows;
        }

        addRow(current);

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We fetch the next block from the appropriate source, if there is one.
            fetchNextBlock(current, queue);
        }
    }

    /// Write the simple aggregation result for the previous group.
    if (merged_rows > 0)
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
        desc.add_function(desc.function.get(), desc.state.data(), &col, cursor->pos, arena.get());
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
