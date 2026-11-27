#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionVector.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyFunctionVector(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionVector(function_name));

    checkNumberOfArguments(function_node, arguments, 1);
    checkArgumentType(function_node, arguments, 0, ResultType::SCALAR, context);
    auto & argument = arguments[0];

    /// Here we only change the result type to INSTANT_VECTOR because all the store methods compatible with ResultType::SCALAR
    /// are compatible with instant vectors too.
    auto res = argument;
    res.node = function_node;
    res.type = ResultType::INSTANT_VECTOR;
    return res;
}

}
