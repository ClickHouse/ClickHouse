#include <Storages/TimeSeries/PrometheusQueryToSQL/applySortFunction.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

bool isSortFunction(std::string_view function_name)
{
    return (function_name == "sort") || (function_name == "sort_desc");
}

SQLQueryPiece applySortFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;

    if (arguments.size() != 1)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects {} argument, but was called with {} arguments",
                        function_name, 1, arguments.size());
    }

    auto & argument = arguments[0];

    if (argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects an argument of type {}, but expression {} has type {}",
                        function_name, ResultType::INSTANT_VECTOR,
                        getPromQLText(argument, context), argument.type);
    }

    /// sort() / sort_desc() do not change the values, they only affect the output ordering.
    /// We pass the argument through unchanged and set the sort direction on the result.
    argument.sort_direction = (function_name == "sort") ? 1 : -1;
    argument.node = function_node;
    return std::move(argument);
}

}
