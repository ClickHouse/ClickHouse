#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionVector.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;
        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];
        if (argument.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::SCALAR,
                            getPromQLText(argument, context), argument.type);
        }
    }
}


SQLQueryPiece applyFunctionVector(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionVector(function_name));

    checkArgumentTypes(function_node, arguments, context);
    auto & argument = arguments[0];

    /// Here we only change the result type to INSTANT_VECTOR because all the store methods compatible with ResultType::SCALAR
    /// are compatible with instant vectors too.
    auto res = argument;
    res.node = function_node;
    res.type = ResultType::INSTANT_VECTOR;
    return res;
}

}
