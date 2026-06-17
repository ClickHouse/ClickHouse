#pragma once

#include <Interpreters/Aggregator.h>

namespace DB
{

struct OutputBlockColumns
{
    MutableColumns key_columns;
    std::vector<IColumn *> raw_key_columns;
    MutableColumns aggregate_columns;
    MutableColumns final_aggregate_columns;
    Aggregator::AggregateColumnsData aggregate_columns_data;
};


OutputBlockColumns prepareOutputBlockColumns(
    const Aggregator::Params & params,
    const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions,
    const Block & res_header,
    Arenas & aggregates_pools,
    bool final,
    size_t rows);

Block finalizeBlock(const Aggregator::Params & params, const Block & res_header, OutputBlockColumns && out_cols, bool final, size_t rows);
}
