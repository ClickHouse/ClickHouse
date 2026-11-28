#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionDoubleExponentialSmoothing.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fixedAtModifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getToGridAggregateFunctionArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/scalarToAST.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyFunctionDoubleExponentialSmoothing(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkNumberOfArguments(function_node, arguments, 3);
    checkArgumentType(function_node, arguments, 0, ResultType::RANGE_VECTOR, context);
    checkArgumentType(function_node, arguments, 1, ResultType::SCALAR, context);
    checkArgumentType(function_node, arguments, 2, ResultType::SCALAR, context);

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto range_argument = std::move(arguments[0]);

    /// The range vector or a factor is empty, so is the result. Both factors are checked before either is converted:
    /// the conversion may register a scalar subquery, which would be left unused if the other factor were empty.
    if (range_argument.store_method == StoreMethod::EMPTY || arguments[1].store_method == StoreMethod::EMPTY
        || arguments[2].store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto smoothing_factor = scalarToAST(std::move(arguments[1]), context);
    auto trend_factor = scalarToAST(std::move(arguments[2]), context);

    checkNoTimeVaryingArgumentWithFixedAt(function_node, range_argument, smoothing_factor, "smoothing factor", context);
    checkNoTimeVaryingArgumentWithFixedAt(function_node, range_argument, trend_factor, "trend factor", context);

    /// A range vector made from a scalar has no `group` column. The result is still a vector grid with one series without
    /// tags, because the aggregate function returns NULL for a window without enough samples and only VECTOR_GRID can hold NULL.
    const bool from_scalar = (range_argument.store_method == StoreMethod::CONST_SCALAR)
        || (range_argument.store_method == StoreMethod::SINGLE_SCALAR) || (range_argument.store_method == StoreMethod::SCALAR_GRID);

    SelectQueryBuilder builder;

    if (from_scalar)
    {
        builder.select_list.push_back(timeSeriesNoTagsGroupToAST());
        builder.select_list.back()->setAlias(ColumnNames::Group);
    }
    else
    {
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    }

    /// timeSeriesDoubleExponentialSmoothingToGrid(<start_timestamp>, <end_timestamp>, <step>, <staleness_window>)(<timestamps>, <values>, <smoothing_factor>, <trend_factor>) AS values
    /// For each grid point it returns the last smoothed value of the window (NULL if the window has fewer than two samples).
    /// Each factor is either one number or, for a factor varying with the evaluation time, an array with one number per
    /// grid point.
    ASTs aggregate_function_arguments = getToGridAggregateFunctionArguments(range_argument, context);
    aggregate_function_arguments.push_back(std::move(smoothing_factor.ast));
    aggregate_function_arguments.push_back(std::move(trend_factor.ast));

    const auto aggregation_range = getRangeAggregationRange(range_argument, node_range, context);
    ASTPtr result_values = addParametersToAggregateFunction(
        makeASTFunction("timeSeriesDoubleExponentialSmoothingToGrid", std::move(aggregate_function_arguments)),
        timeSeriesTimestampToAST(aggregation_range.start_time, context.result_timestamp_type),
        timeSeriesTimestampToAST(aggregation_range.end_time, context.result_timestamp_type),
        timeSeriesDurationToAST(aggregation_range.step, context.result_timestamp_type),
        timeSeriesDurationToAST(node_range.window, context.result_timestamp_type));

    /// With constant factors, a fixed @ on the range vector makes the whole call step-invariant in PromQL, so it is evaluated once.
    const auto * const fixed_at_node = getFixedAtModifier(range_argument);
    if (fixed_at_node)
        result_values = repeatFixedAtResultOverGrid(
            std::move(result_values), aggregation_range, stepsInTimeSeriesRange(node_range.start_time, node_range.end_time, node_range.step));

    builder.select_list.push_back(std::move(result_values));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    if (!from_scalar)
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    if (range_argument.select_query)
    {
        auto & subqueries = context.subqueries;
        subqueries.emplace_back(subqueries.size(), std::move(range_argument.select_query), SQLSubqueryType::TABLE);
        builder.from_table = subqueries.back().name;
    }

    SQLQueryPiece res = range_argument;
    res.store_method = StoreMethod::VECTOR_GRID;
    res.scalar_value = {};
    res.node = function_node;

    res.select_query = builder.getSelectQuery();
    res.type = ResultType::INSTANT_VECTOR;
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;

    /// `double_exponential_smoothing` always drops the metric name (PromQL: function outputs have no `__name__`). A scalar has no metric name.
    if (from_scalar)
        res.metric_name_dropped = true;
    else
        res = dropMetricName(std::move(res), context);

    return res;
}

}
