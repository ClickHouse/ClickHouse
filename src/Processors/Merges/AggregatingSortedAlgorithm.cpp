#include <Processors/Merges/AggregatingSortedAlgorithm.h>

#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace
{
    AggregatingSortedAlgorithm::ColumnsDefinition defineColumns(
        const Block & header, const SortDescription & description)
    {
        AggregatingSortedAlgorithm::ColumnsDefinition def = {};
        size_t num_columns = header.columns();

        /// Fill in the column numbers that need to be aggregated.
        for (size_t i = 0; i < num_columns; ++i)
        {
            const ColumnWithTypeAndName & column = header.safeGetByPosition(i);

            /// We leave only states of aggregate functions.
            if (!dynamic_cast<const DataTypeAggregateFunction *>(column.type.get())
                && !dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
            {
                def.column_numbers_not_to_aggregate.push_back(i);
                continue;
            }

            /// Included into PK?
            auto it = description.begin();
            for (; it != description.end(); ++it)
                if (it->column_name == column.name || (it->column_name.empty() && it->column_number == i))
                    break;

            if (it != description.end())
            {
                def.column_numbers_not_to_aggregate.push_back(i);
                continue;
            }

            if (auto simple_aggr = dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
            {
                auto type = recursiveRemoveLowCardinality(column.type);
                if (type.get() == column.type.get())
                    type = nullptr;

                // simple aggregate function
                AggregatingSortedAlgorithm::SimpleAggregateDescription desc(simple_aggr->getFunction(), i, type);
                if (desc.function->allocatesMemoryInArena())
                    def.allocates_memory_in_arena = true;

                def.columns_to_simple_aggregate.emplace_back(std::move(desc));
            }
            else
            {
                // standard aggregate function
                def.columns_to_aggregate.emplace_back(i);
            }
        }

        return def;
    }

    MutableColumns getMergedColumns(const Block & header, const AggregatingSortedAlgorithm::ColumnsDefinition & def)
    {
        MutableColumns columns;
        columns.resize(header.columns());

        for (auto & desc : def.columns_to_simple_aggregate)
        {
            auto & type = header.getByPosition(desc.column_number).type;
            columns[desc.column_number] = recursiveRemoveLowCardinality(type)->createColumn();
        }

        for (size_t i = 0; i < columns.size(); ++i)
            if (!columns[i])
                columns[i] = header.getByPosition(i).type->createColumn();

        return columns;
    }
}

AggregatingSortedAlgorithm::AggregatingSortedAlgorithm(
    const Block & header_, size_t num_inputs,
    SortDescription description_, size_t max_block_size)
    : IMergingAlgorithmWithDelayedChunk(num_inputs, std::move(description_))
    , header(header_)
    , columns_definition(defineColumns(header, description_))
    , merged_data(getMergedColumns(header, columns_definition), max_block_size, columns_definition)
{
}

void AggregatingSortedAlgorithm::prepareChunk(Chunk & chunk) const
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    for (auto & desc : columns_definition.columns_to_simple_aggregate)
        if (desc.inner_type)
            columns[desc.column_number] = recursiveRemoveLowCardinality(columns[desc.column_number]);

    chunk.setColumns(std::move(columns), num_rows);
}

void AggregatingSortedAlgorithm::initialize(Chunks chunks)
{
    for (auto & chunk : chunks)
        if (chunk)
            prepareChunk(chunk);

    initializeQueue(std::move(chunks));
}

void AggregatingSortedAlgorithm::consume(Chunk chunk, size_t source_num)
{
    prepareChunk(chunk);
    updateCursor(std::move(chunk), source_num);
}

IMergingAlgorithm::Status AggregatingSortedAlgorithm::merge()
{
    /// We take the rows in the correct order and put them in `merged_block`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        bool key_differs;
        bool has_previous_group = !last_key.empty();

        SortCursor current = queue.current();

        {
            detail::RowRef current_key;
            current_key.set(current);

            if (!has_previous_group)    /// The first key encountered.
                key_differs = true;
            else
                key_differs = !last_key.hasEqualSortColumnsWith(current_key);

            last_key = current_key;
            last_chunk_sort_columns.clear();
        }

        if (key_differs)
        {
            /// Write the simple aggregation result for the previous group.
            if (merged_data.isGroupStarted())
            {
                insertSimpleAggregationResult();
                merged_data.insertRow();
            }

            /// if there are enough rows accumulated and the last one is calculated completely
            if (merged_data.hasEnoughRows())
            {
                last_key.reset();
                Status(merged_data.pull(columns_definition, header));
            }

            /// We will write the data for the group. We copy the values of ordinary columns.
            merged_data.initializeRow(current->all_columns, current->pos,
                                      columns_definition.column_numbers_not_to_aggregate);

            /// Add the empty aggregation state to the aggregate columns. The state will be updated in the `addRow` function.
            for (auto & column_to_aggregate : columns_definition.columns_to_aggregate)
                column_to_aggregate.column->insertDefault();

            /// Reset simple aggregation states for next row
            for (auto & desc : columns_definition.columns_to_simple_aggregate)
                desc.createState();

            if (columns_definition.allocates_memory_in_arena)
                arena = std::make_unique<Arena>();
        }

        addRow(current);

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    /// Write the simple aggregation result for the previous group.
    if (merged_data.isGroupStarted())
    {
        insertSimpleAggregationResult();
        merged_data.insertRow();
    }

    last_chunk_sort_columns.clear();
    return Status(merged_data.pull(columns_definition, header), true);
}

void AggregatingSortedAlgorithm::addRow(SortCursor & cursor)
{
    for (auto & desc : columns_definition.columns_to_aggregate)
        desc.column->insertMergeFrom(*cursor->all_columns[desc.column_number], cursor->pos);

    for (auto & desc : columns_definition.columns_to_simple_aggregate)
    {
        auto & col = cursor->all_columns[desc.column_number];
        desc.add_function(desc.function.get(), desc.state.data(), &col, cursor->pos, arena.get());
    }
}

void AggregatingSortedAlgorithm::insertSimpleAggregationResult()
{
    for (auto & desc : columns_definition.columns_to_simple_aggregate)
    {
        desc.function->insertResultInto(desc.state.data(), *desc.column);
        desc.destroyState();
    }
}


}
