#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionPi.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>
#include <numbers>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece fromFunctionPi(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    chassert(isFunctionPi(function_node->function_name));

    checkNumberOfArguments(function_node, arguments, 0);

    /// This implementation is similar to the implementation of function fromLiteral().
    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::SCALAR, StoreMethod::EMPTY};

    SQLQueryPiece res{function_node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
    res.scalar_value = std::numbers::pi;
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;

    return res;
}

}
