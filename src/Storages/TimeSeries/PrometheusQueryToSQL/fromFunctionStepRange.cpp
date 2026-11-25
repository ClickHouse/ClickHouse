#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionStepRange.h>

#include <Core/DecimalFunctions.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece fromFunctionStepRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionStepRange(function_name));

    checkNumberOfArguments(function_node, arguments, 0);

    const auto & node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::SCALAR, StoreMethod::EMPTY};

    /// A subquery changes the node's grid, but not the outer query's metadata.
    const auto & query_range = context.node_range_getter.get(context.promql_tree->getRoot());

    SQLQueryPiece res{function_node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;

    if (function_name == "step")
    {
        /// A one-point range query still has its requested step. An instant query has step 0.
        res.scalar_value = DecimalUtils::convertTo<Float64>(query_range.step, context.result_timestamp_scale);
    }
    else
    {
        /// Subtract ticks before converting to seconds so modern timestamps keep small ranges precise.
        const Decimal128 duration{Int128(query_range.end_time.value) - Int128(query_range.start_time.value)};
        res.scalar_value = DecimalUtils::convertTo<Float64>(duration, context.result_timestamp_scale);
    }

    return res;
}

}
