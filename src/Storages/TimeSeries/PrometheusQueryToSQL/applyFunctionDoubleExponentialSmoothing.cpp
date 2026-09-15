#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionDoubleExponentialSmoothing.h>

#include <Common/Exception.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>

#include <vector>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks that the argument types are valid for `double_exponential_smoothing`:
    /// a range vector followed by two scalars.
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 3)
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 3 arguments, but was called with {} arguments",
                            function_name, arguments.size());

        const auto & vector_arg = arguments[0];
        if (vector_arg.type != ResultType::RANGE_VECTOR)
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::RANGE_VECTOR,
                            getPromQLText(vector_arg, context), vector_arg.type);

        for (size_t i = 1; i <= 2; ++i)
        {
            const auto & scalar_arg = arguments[i];
            if (scalar_arg.type != ResultType::SCALAR)
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument {} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::SCALAR,
                                getPromQLText(scalar_arg, context), scalar_arg.type);
        }
    }

    Float64 extractConstantFactor(
        const PrometheusQueryTree::Function * function_node, const SQLQueryPiece & arg, std::string_view factor_name)
    {
        const auto & function_name = function_node->function_name;

        if (arg.store_method != StoreMethod::CONST_SCALAR)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Function '{}' currently requires a constant {} parameter", function_name, factor_name);

        const Float64 value = arg.scalar_value;
        if (!(value > 0 && value < 1))
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Function '{}' expects {} in the open interval (0, 1), got {}", function_name, factor_name, value);

        return value;
    }
}


bool isDoubleExponentialSmoothing(std::string_view function_name)
{
    return function_name == "double_exponential_smoothing";
}


SQLQueryPiece applyDoubleExponentialSmoothing(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    const Float64 smoothing_factor = extractConstantFactor(function_node, arguments[1], "smoothing factor");
    const Float64 trend_factor = extractConstantFactor(function_node, arguments[2], "trend factor");

    std::vector<ASTPtr> extra_params;
    extra_params.push_back(make_intrusive<ASTLiteral>(smoothing_factor));
    extra_params.push_back(make_intrusive<ASTLiteral>(trend_factor));

    /// double_exponential_smoothing drops the metric name in PromQL, like other transforming functions.
    return applyAggregateFunctionOverRange(
        function_node, "timeSeriesDoubleExponentialSmoothingToGrid", /* drop_metric_name = */ true,
        std::move(arguments[0]), std::move(extra_params), context);
}

}
