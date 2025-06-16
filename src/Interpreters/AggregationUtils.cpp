#include <AggregateFunctions/IAggregateFunction.h>
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
    const Block & res_header,
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
        key_columns[i] = res_header.safeGetByPosition(i).type->createColumn();
        key_columns[i]->reserve(rows);
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (!final)
        {
            const auto & aggregate_column_name = params.aggregates[i].column_name;
            aggregate_columns[i] = res_header.getByName(aggregate_column_name).type->createColumn();

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
                final_aggregate_columns[i]->forEachSubcolumnRecursively(callback);
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

Block finalizeBlock(const Aggregator::Params & params, const Block & res_header, OutputBlockColumns && out_cols, bool final, size_t rows)
{
    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    Block res = res_header.cloneEmpty();

    for (size_t i = 0; i < params.keys_size; ++i)
        res.getByPosition(i).column = std::move(key_columns[i]);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        if (final)
            res.getByName(aggregate_column_name).column = std::move(final_aggregate_columns[i]);
        else
            res.getByName(aggregate_column_name).column = std::move(aggregate_columns[i]);
    }

    /// Change the size of the columns-constants in the block.
    size_t columns = res_header.columns();
    for (size_t i = 0; i < columns; ++i)
        if (isColumnConst(*res.getByPosition(i).column))
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

    return res;
}
}
