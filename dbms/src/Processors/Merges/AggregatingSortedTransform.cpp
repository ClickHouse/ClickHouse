#include <Processors/Merges/AggregatingSortedTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    AggregatingSortedTransform::ColumnsDefinition defineColumns(
        const Block & header, const SortDescription & description)
    {
        AggregatingSortedTransform::ColumnsDefinition def = {};
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
                AggregatingSortedTransform::SimpleAggregateDescription desc(simple_aggr->getFunction(), i, type);
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
}

AggregatingSortedTransform::AggregatingSortedTransform(
    const Block & header, size_t num_inputs,
    SortDescription description_, size_t max_block_size)
    : IMergingTransform(num_inputs, header, header, true)
    , columns_definition(defineColumns(header, description_))
    , merged_data(header.cloneEmptyColumns(), false, max_block_size)
    , description(std::move(description_))
    , source_chunks(num_inputs)
    , cursors(num_inputs)
{
    merged_data.initAggregateDescription(columns_definition);
}

void AggregatingSortedTransform::initializeInputs()
{
    queue = SortingHeap<SortCursor>(cursors);
    is_queue_initialized = true;
}

void AggregatingSortedTransform::consume(Chunk chunk, size_t input_number)
{
    updateCursor(std::move(chunk), input_number);

    if (is_queue_initialized)
        queue.push(cursors[input_number]);
}

void AggregatingSortedTransform::updateCursor(Chunk chunk, size_t source_num)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    for (auto & desc : columns_definition.columns_to_simple_aggregate)
        if (desc.type_to_convert)
            columns[desc.column_number] = recursiveRemoveLowCardinality(columns[desc.column_number]);

    chunk.setColumns(std::move(columns), num_rows);

    auto & source_chunk = source_chunks[source_num];

    if (source_chunk)
    {
        /// Extend lifetime of last chunk.
        last_chunk = std::move(source_chunk);
        last_chunk_sort_columns = std::move(cursors[source_num].all_columns);

        source_chunk = std::move(chunk);
        cursors[source_num].reset(source_chunk.getColumns(), {});
    }
    else
    {
        if (cursors[source_num].has_collation)
            throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

        source_chunk = std::move(chunk);
        cursors[source_num] = SortCursorImpl(source_chunk.getColumns(), description, source_num);
    }
}

void AggregatingSortedTransform::work()
{
    merge();
    prepareOutputChunk(merged_data);

    if (has_output_chunk)
    {
        size_t num_rows = output_chunk.getNumRows();
        auto columns = output_chunk.detachColumns();
        auto & header = getOutputs().back().getHeader();

        for (auto & desc : columns_definition.columns_to_simple_aggregate)
        {
            if (desc.type_to_convert)
            {
                auto & from_type = header.getByPosition(desc.column_number).type;
                auto & to_type = desc.type_to_convert;
                columns[desc.column_number] = recursiveTypeConversion(columns[desc.column_number], from_type, to_type);
            }
        }

        output_chunk.setColumns(std::move(columns), num_rows);

        merged_data.initAggregateDescription(columns_definition);
    }
}

void AggregatingSortedTransform::merge()
{
    /// We take the rows in the correct order and put them in `merged_block`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        bool key_differs;
        bool has_previous_group = !last_key.empty();

        SortCursor current = queue.current();
        detail::RowRef current_key;
        current_key.set(current);

        if (!has_previous_group)    /// The first key encountered.
            key_differs = true;
        else
            key_differs = !last_key.hasEqualSortColumnsWith(current_key);

        if (key_differs)
        {
            /// if there are enough rows accumulated and the last one is calculated completely
            if (merged_data.hasEnoughRows())
            {
                /// Write the simple aggregation result for the previous group.
                insertSimpleAggregationResult();
                last_key.reset();
                return;
            }

            /// We will write the data for the group. We copy the values of ordinary columns.
            merged_data.insertRow(current->all_columns, current->pos,
                                  columns_definition.column_numbers_not_to_aggregate);

            /// Add the empty aggregation state to the aggregate columns. The state will be updated in the `addRow` function.
            for (auto & column_to_aggregate : columns_definition.columns_to_aggregate)
                column_to_aggregate.column->insertDefault();

            /// Write the simple aggregation result for the previous group.
            if (merged_data.mergedRows() > 0)
                insertSimpleAggregationResult();

            /// Reset simple aggregation states for next row
            for (auto & desc : columns_definition.columns_to_simple_aggregate)
                desc.createState();

            if (columns_definition.allocates_memory_in_arena)
                arena = std::make_unique<Arena>();
        }

        addRow(current);

        if (!current->isLast())
        {
            last_key = current_key;
            last_chunk_sort_columns.clear();
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

    /// Write the simple aggregation result for the previous group.
    if (merged_data.mergedRows() > 0)
        insertSimpleAggregationResult();

    last_chunk_sort_columns.clear();
    is_finished = true;
}

void AggregatingSortedTransform::addRow(SortCursor & cursor)
{
    for (auto & desc : columns_definition.columns_to_aggregate)
        desc.column->insertMergeFrom(*cursor->all_columns[desc.column_number], cursor->pos);

    for (auto & desc : columns_definition.columns_to_simple_aggregate)
    {
        auto & col = cursor->all_columns[desc.column_number];
        desc.add_function(desc.function.get(), desc.state.data(), &col, cursor->pos, arena.get());
    }
}

void AggregatingSortedTransform::insertSimpleAggregationResult()
{
    for (auto & desc : columns_definition.columns_to_simple_aggregate)
    {
        desc.function->insertResultInto(desc.state.data(), *desc.column);
        desc.destroyState();
    }
}

}
