#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

void checkNumberOfArguments(const PrometheusQueryTree::Node * node, const std::vector<SQLQueryPiece> & arguments, size_t expected_count)
{
    if (arguments.size() != expected_count)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} expects {} argument(s), but was called with {}",
                        node->getTypeAndName(), expected_count, arguments.size());
    }
}

void checkNumberOfArgumentsBetween(
    const PrometheusQueryTree::Node * node, const std::vector<SQLQueryPiece> & arguments, size_t min_count, size_t max_count)
{
    if ((arguments.size() < min_count) || (arguments.size() > max_count))
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} expects from {} to {} argument(s), but was called with {}",
                        node->getTypeAndName(), min_count, max_count, arguments.size());
    }
}

void checkNumberOfArgumentsAtLeast(const PrometheusQueryTree::Node * node, const std::vector<SQLQueryPiece> & arguments, size_t min_count)
{
    if (arguments.size() < min_count)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} expects {} or more argument(s), but was called with {}",
                        node->getTypeAndName(), min_count, arguments.size());
    }
}

void checkArgumentType(
    const PrometheusQueryTree::Node * node,
    const std::vector<SQLQueryPiece> & arguments,
    size_t index,
    ResultType expected_type,
    const ConverterContext & context)
{
    const auto & argument = arguments.at(index);

    if (argument.type != expected_type)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} expects argument #{} of type {}, but expression {} has type {}",
                        node->getTypeAndName(), index + 1, expected_type,
                        getPromQLText(argument, context), argument.type);
    }
}

}
