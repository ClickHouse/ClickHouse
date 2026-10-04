#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionQuantileOverTime.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fixedAtModifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropHistogramValues.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getToGridAggregateFunctionArguments.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{

/// Checks that the arguments are valid for `quantile_over_time`:
///   - exactly 2 arguments
///   - first argument is a SCALAR (the quantile parameter `phi`)
///   - second argument is a RANGE_VECTOR
void checkArgumentTypes(std::string_view function_name, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
{
    if (arguments.size() != 2)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects 2 arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

    const auto & phi_argument = arguments[0];
    if (phi_argument.type != ResultType::SCALAR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function {} expects first argument of type {}, but expression {} has type {}",
                        function_name, ResultType::SCALAR,
                        getPromQLText(phi_argument, context), phi_argument.type);
    }

    const auto & range_argument = arguments[1];
    if (range_argument.type != ResultType::RANGE_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function {} expects second argument of type {}, but expression {} has type {}",
                        function_name, ResultType::RANGE_VECTOR,
                        getPromQLText(range_argument, context), range_argument.type);
    }
}


/// The quantile level (the 1st argument of `quantile_over_time`) converted to an AST.
struct QuantileLevel
{
    /// nullptr if the level is statically empty, then the result of `quantile_over_time` is empty too.
    ASTPtr ast;

    /// Whether the level is the same at every grid point. Then `ast` is a number: a literal or a reference to a single-row
    /// scalar subquery. Otherwise (e.g. `time()` in a range query) it varies with the evaluation time and `ast` is an array
    /// with one value per grid point.
    bool is_constant = true;
};

QuantileLevel getQuantileLevel(SQLQueryPiece & scalar_argument, ConverterContext & context)
{
    QuantileLevel quantile_level;
    switch (scalar_argument.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        {
            /// Keep the literal at Float64: casting phi to a Float32 scalar type would round
            /// e.g. 1.00000003 to 1.0 and hide the phi > 1 edge case.
            quantile_level.ast = make_intrusive<ASTLiteral>(scalar_argument.scalar_value);
            break;
        }

        case StoreMethod::SINGLE_SCALAR:
        {
            /// A scalar subquery is nullable, but it always has exactly one row here.
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
            quantile_level.ast = makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(context.subqueries.back().name));
            break;
        }

        case StoreMethod::SCALAR_GRID:
        {
            /// A scalar grid is one row with one Array column, so it is a scalar subquery too.
            quantile_level.is_constant = false;
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
            quantile_level.ast = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
            break;
        }

        case StoreMethod::EMPTY:
        {
            break;
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::VECTOR_GRID:
        case StoreMethod::RAW_DATA:
        case StoreMethod::HISTOGRAM_RAW_DATA:
        case StoreMethod::HISTOGRAM_GRID:
        {
            /// Can't get in here because these store methods are incompatible with a scalar (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(scalar_argument, context);
        }
    }
    return quantile_level;
}

}


bool isFunctionQuantileOverTime(std::string_view function_name)
{
    return function_name == "quantile_over_time";
}


SQLQueryPiece applyFunctionQuantileOverTime(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto function_name = function_node->function_name;
    checkArgumentTypes(function_name, arguments, context);

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto range_argument = std::move(arguments[1]);

    if (range_argument.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY}; /// The range vector is empty, so is the result.

    /// The level goes last: it may register a scalar subquery, which is left unused if the result is found empty above.
    QuantileLevel quantile_level = getQuantileLevel(arguments[0], context);
    if (!quantile_level.ast)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    /// This function is defined on float samples only, so the native-histogram samples are ignored (Prometheus semantics).
    range_argument = dropHistogramSamples(std::move(range_argument), context);

    ASTs aggregate_function_arguments = getToGridAggregateFunctionArguments(range_argument, context);

    /// With a constant level, a fixed @ on the range vector makes the whole call step-invariant in PromQL, so it is evaluated once.
    const auto * fixed_at_node = getFixedAtModifier(range_argument);
    if (fixed_at_node && !quantile_level.is_constant)
    {
        /// A fixed @ freezes the samples but not phi, so PromQL still evaluates per step; the aggregate derives its
        /// window from each grid point and cannot express a frozen window with a per-point quantile level.
        /// To implement this we need another aggregate function: not over a grid of timestamps like timeSeriesQuantileToGrid,
        /// but over an array of levels, returning the quantile of the single frozen window for each of them.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Function '{}' does not support a time-varying first argument (the quantile) together with "
                        "a fixed @ modifier on the range vector {}",
                        function_name, getPromQLText(range_argument, context));
    }
    const auto aggregation_range = getRangeAggregationRange(fixed_at_node, node_range, context);

    /// The result is a vector grid (one row per series, the aggregate function is calculated `GROUP BY group`) if the
    /// range vector holds series, and a scalar grid if it was made from a scalar.
    const bool has_group = (range_argument.store_method == StoreMethod::VECTOR_GRID) || (range_argument.store_method == StoreMethod::RAW_DATA);

    SelectQueryBuilder builder;

    if (has_group)
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    /// timeSeriesQuantileToGrid(<start_timestamp>, <end_timestamp>, <step>, <staleness_window>)(<timestamps>, <values>, <phi>) AS values
    /// For each grid point it returns the phi-quantile of the values inside that window (NULL if the window is empty),
    /// with the Prometheus edge cases of `phi` (below 0 gives -Inf, above 1 gives +Inf, NaN gives NaN).
    /// `phi` is either one number or, for a quantile level varying with the evaluation time, an array with one number per
    /// grid point.
    aggregate_function_arguments.push_back(std::move(quantile_level.ast));
    ASTPtr result_values = addParametersToAggregateFunction(
        makeASTFunction("timeSeriesQuantileToGrid", std::move(aggregate_function_arguments)),
        timeSeriesTimestampToAST(aggregation_range.start_time, context.result_timestamp_type),
        timeSeriesTimestampToAST(aggregation_range.end_time, context.result_timestamp_type),
        timeSeriesDurationToAST(aggregation_range.step, context.result_timestamp_type),
        timeSeriesDurationToAST(window, context.result_timestamp_type));

    if (fixed_at_node)
        result_values = repeatFixedAtResultOverGrid(
            std::move(result_values), aggregation_range, stepsInTimeSeriesRange(start_time, end_time, step));

    builder.select_list.push_back(std::move(result_values));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    if (has_group)
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    if (range_argument.select_query)
    {
        auto & subqueries = context.subqueries;
        subqueries.emplace_back(subqueries.size(), std::move(range_argument.select_query), SQLSubqueryType::TABLE);
        builder.from_table = subqueries.back().name;
    }

    SQLQueryPiece res = range_argument;
    res.store_method = has_group ? StoreMethod::VECTOR_GRID : StoreMethod::SCALAR_GRID;
    res.scalar_value = {};
    res.node = function_node;

    res.select_query = builder.getSelectQuery();
    res.type = ResultType::INSTANT_VECTOR;
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;

    /// `quantile_over_time` always drops the metric name (PromQL: function outputs have no `__name__`).
    if (has_group)
        res = dropMetricName(std::move(res), context);

    return res;
}

}
