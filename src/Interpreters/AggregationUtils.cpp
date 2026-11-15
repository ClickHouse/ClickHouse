#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Interpreters/AggregationUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

OutputBlockColumns prepareOutputBlockColumns(
    const Aggregator::Params & params,
    const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions,
    const DataTypes & key_types,
    const DataTypes & aggregate_state_types,
    Arenas & aggregates_pools,
    bool final,
    size_t rows)
{
    MutableColumns key_columns(params.keys_size);
    MutableColumns aggregate_columns(params.aggregates_size);
    MutableColumns final_aggregate_columns(params.aggregates_size);
    Aggregator::AggregateColumnsData aggregate_columns_data(params.aggregates_size);

    for (size_t i = 0; i < params.keys_size; ++i)
    {
        key_columns[i] = key_types[i]->createColumn();
        key_columns[i]->reserve(rows);
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (!final)
        {
            aggregate_columns[i] = aggregate_state_types[i]->createColumn();

            /// The ColumnAggregateFunction column captures the shared ownership of the arena with the aggregate function states.
            ColumnAggregateFunction & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);

            for (auto & pool : aggregates_pools)
                column_aggregate_func.addArena(pool);

            aggregate_columns_data[i] = &column_aggregate_func.getData();
            aggregate_columns_data[i]->reserve(rows);
        }
        else
        {
            final_aggregate_columns[i] = aggregate_functions[i]->getResultType()->createColumn();
            final_aggregate_columns[i]->reserve(rows);

            if (aggregate_functions[i]->isState())
            {
                auto callback = [&](IColumn & subcolumn)
                {
                    /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
                    if (auto * column_aggregate_func = typeid_cast<ColumnAggregateFunction *>(&subcolumn))
                        for (auto & pool : aggregates_pools)
                            column_aggregate_func->addArena(pool);
                };

                callback(*final_aggregate_columns[i]);
                final_aggregate_columns[i]->forEachMutableSubcolumnRecursively(callback);
            }
        }
    }

    if (key_columns.size() != params.keys_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregate. Unexpected key columns size.");

    std::vector<IColumn *> raw_key_columns;
    raw_key_columns.reserve(key_columns.size());
    for (auto & column : key_columns)
        raw_key_columns.push_back(column.get());

    return {
        .key_columns = std::move(key_columns),
        .raw_key_columns = std::move(raw_key_columns),
        .aggregate_columns = std::move(aggregate_columns),
        .final_aggregate_columns = std::move(final_aggregate_columns),
        .aggregate_columns_data = std::move(aggregate_columns_data),
    };
}

Chunk finalizeChunk(
    const Aggregator::Params & params,
    OutputBlockColumns && out_cols,
    bool final)
{
    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    Columns columns;
    columns.reserve(params.keys_size + params.aggregates_size);

    for (size_t i = 0; i < params.keys_size; ++i)
        columns.emplace_back(std::move(key_columns[i]));

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (final)
            columns.emplace_back(std::move(final_aggregate_columns[i]));
        else
            columns.emplace_back(std::move(aggregate_columns[i]));
    }

    size_t rows = columns.empty() ? 0 : columns[0]->size();
    return Chunk(std::move(columns), rows);
}
}
