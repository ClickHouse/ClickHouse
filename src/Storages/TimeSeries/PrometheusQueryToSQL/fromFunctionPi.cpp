#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionPi.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <numbers>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece fromFunctionPi(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionPi(function_name));

    if (!arguments.empty())
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects no arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

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
