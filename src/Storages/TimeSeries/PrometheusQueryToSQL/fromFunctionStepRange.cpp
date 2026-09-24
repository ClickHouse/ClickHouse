#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionStepRange.h>

#include <Common/Exception.h>
#include <Core/DecimalFunctions.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece fromFunctionStepRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionStepRange(function_name));

    if (!arguments.empty())
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects no arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

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
        /// The root range retains the requested step even for a one-point range query.
        /// Instant queries have a zero step, including when they contain subqueries.
        res.scalar_value = DecimalUtils::convertTo<Float64>(query_range.step, context.timestamp_scale);
    }
    else
    {
        /// Subtract the ticks before converting to seconds, without narrowing a large span.
        const Decimal128 duration{Int128(query_range.end_time.value) - Int128(query_range.start_time.value)};
        res.scalar_value = DecimalUtils::convertTo<Float64>(duration, context.timestamp_scale);
    }

    return res;
}

}
