#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionDoubleExponentialSmoothing.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fixedAtModifier.h>

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

    /// A factor of `double_exponential_smoothing` converted to an AST, like the level of `quantile_over_time`.
    struct Factor
    {
        ASTPtr ast;

        /// Whether the factor is the same at every grid point. Then `ast` is a number: a literal or a reference to a single-row
        /// scalar subquery. Otherwise (e.g. `time()` in a range query) `ast` is an array with one value per grid point.
        bool is_constant = true;
    };

    Factor getFactor(SQLQueryPiece & scalar_argument, ConverterContext & context)
    {
        Factor factor;
        switch (scalar_argument.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                factor.ast = make_intrusive<ASTLiteral>(scalar_argument.scalar_value);
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                /// A scalar subquery is nullable, but it always has exactly one row here.
                context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
                factor.ast = makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(context.subqueries.back().name));
                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// A scalar grid is one row with one Array column, so it is a scalar subquery too.
                factor.is_constant = false;
                context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
                factor.ast = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                break;
            }

            case StoreMethod::EMPTY:
            case StoreMethod::CONST_STRING:
            case StoreMethod::VECTOR_GRID:
            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here: an empty factor is handled by the caller, the others are incompatible with a scalar.
                throwUnexpectedStoreMethod(scalar_argument, context);
            }
        }
        return factor;
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

    /// The factors are empty if the evaluation range is empty (e.g. a subquery window without steps), then so is the result.
    /// Like Prometheus, the factors are checked only where a window has samples, so an empty range vector is not an error.
    if (context.node_range_getter.get(function_node).empty() || (arguments[0].store_method == StoreMethod::EMPTY)
        || (arguments[1].store_method == StoreMethod::EMPTY) || (arguments[2].store_method == StoreMethod::EMPTY))
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    Factor smoothing_factor = getFactor(arguments[1], context);
    Factor trend_factor = getFactor(arguments[2], context);

    if (getFixedAtModifier(arguments[0]) && !(smoothing_factor.is_constant && trend_factor.is_constant))
    {
        /// A fixed @ freezes the samples but not the factors, and the aggregate can't apply per-step factors to one frozen window.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Function '{}' does not support a time-varying smoothing or trend factor together with "
                        "a fixed @ modifier on the range vector {}",
                        function_node->function_name, getPromQLText(arguments[0], context));
    }

    ASTs extra_arguments;
    extra_arguments.push_back(std::move(smoothing_factor.ast));
    extra_arguments.push_back(std::move(trend_factor.ast));

    /// double_exponential_smoothing drops the metric name in PromQL, like other transforming functions.
    return applyAggregateFunctionOverRange(
        function_node, "timeSeriesDoubleExponentialSmoothingToGrid", /* drop_metric_name = */ true,
        /* needs_cast_to_float64 = */ false, std::move(arguments[0]), std::move(extra_arguments), context);
}

}
