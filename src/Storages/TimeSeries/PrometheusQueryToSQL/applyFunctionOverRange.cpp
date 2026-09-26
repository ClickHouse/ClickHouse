#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getToGridAggregateFunctionArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fixedAtModifier.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the function.
    void checkArgumentTypes(std::string_view function_name, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        size_t expected_number_of_arguments = 1;

        if (arguments.size() != expected_number_of_arguments)
        {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects {} {}, got {} arguments",
                                function_name, expected_number_of_arguments, (expected_number_of_arguments == 1 ? "argument" : "arguments"),
                                arguments.size());
        }

        const auto & argument = arguments[0];
        if (argument.type != ResultType::RANGE_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function {} expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::RANGE_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
        bool drop_metric_name = true;

        /// The aggregate function returns a sample's value (can be Float32), a sample's timestamp (a DateTime type) or a count (UInt64)
        /// instead of Float64, so the result must be cast.
        bool needs_cast_to_float64 = false;
    };

    /// Returns information about how the specified prometheus function is implemented.
    /// Returns nullptr if not found.
    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"rate",
             {
                 "timeSeriesRateToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"increase",
             {
                 "timeSeriesIncreaseToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"irate",
             {
                 "timeSeriesInstantRateToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"delta",
             {
                 "timeSeriesDeltaToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"idelta",
             {
                 "timeSeriesInstantDeltaToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"first_over_time",
             {
                 "timeSeriesFirstToGrid",
                 /* drop_metric_name = */ false,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"ts_of_first_over_time",
             {
                 "timeSeriesTimestampOfFirstToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"last_over_time",
             {
                 "timeSeriesLastToGrid",
                 /* drop_metric_name = */ false,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"ts_of_last_over_time",
             {
                 "timeSeriesTimestampOfLastToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"max_over_time",
             {
                 "timeSeriesMaxToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"ts_of_max_over_time",
             {
                 "timeSeriesTimestampOfMaxToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"min_over_time",
             {
                 "timeSeriesMinToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"ts_of_min_over_time",
             {
                 "timeSeriesTimestampOfMinToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"present_over_time",
             {
                 "timeSeriesPresentToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"deriv",
             {
                 "timeSeriesDerivToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"changes",
             {
                 "timeSeriesChangesToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"resets",
             {
                 "timeSeriesResetsToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"sum_over_time",
             {
                 "timeSeriesSumToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"avg_over_time",
             {
                 "timeSeriesAvgToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"count_over_time",
             {
                 "timeSeriesCountToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ true,
             }},

            {"mad_over_time",
             {
                 "timeSeriesMadToGrid",
                 /* drop_metric_name = */ true,
             }},

            /// TODO:
            /// stddev_over_time
            /// stdvar_over_time
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isFunctionOverRange(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    return applyFunctionOverRange(function_node, function_node->function_name, std::move(arguments), context);
}


SQLQueryPiece applyFunctionOverRange(
    const Node * node,
    std::string_view function_name,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context,
    std::optional<bool> drop_metric_name)
{
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_name, arguments, context);

    auto node_range = context.node_range_getter.get(node);
    if (node_range.empty())
        return SQLQueryPiece{node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto argument = std::move(arguments[0]);

    if (argument.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY}; /// The range vector is empty, so is the result.

    ASTs aggregate_function_arguments = getToGridAggregateFunctionArguments(argument, context);

    const auto * fixed_at_node = getFixedAtModifier(argument);
    const auto aggregation_range = getRangeAggregationRange(fixed_at_node, node_range, context);

    /// The result is a vector grid (one row per series, the aggregate function is calculated `GROUP BY group`) if the
    /// range vector holds series, and a scalar grid if it was made from a scalar.
    const bool has_group = (argument.store_method == StoreMethod::VECTOR_GRID) || (argument.store_method == StoreMethod::RAW_DATA);

    SelectQueryBuilder builder;

    if (has_group)
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    /// <aggregate_function>(<timestamps>, <values>) AS values
    ASTPtr aggregate_values = addParametersToAggregateFunction(
        makeASTFunction(impl_info->ch_function_name, std::move(aggregate_function_arguments)),
        timeSeriesTimestampToAST(aggregation_range.start_time, context.result_timestamp_type),
        timeSeriesTimestampToAST(aggregation_range.end_time, context.result_timestamp_type),
        timeSeriesDurationToAST(aggregation_range.step, context.result_timestamp_type),
        timeSeriesDurationToAST(window, context.result_timestamp_type));

    if (impl_info->needs_cast_to_float64)
    {
        /// CAST(<aggregate_function>(timestamp, value), 'Array(Nullable(Float64))')
        /// See `needs_cast_to_float64`; a timestamp becomes seconds since 1970-01-01 as in Prometheus. The cast does nothing
        /// for Float64 and is cheap anyway: the aggregated grid is much smaller than the raw data.
        aggregate_values = makeASTFunction("CAST", std::move(aggregate_values), make_intrusive<ASTLiteral>("Array(Nullable(Float64))"));
    }

    if (fixed_at_node)
        aggregate_values = repeatFixedAtResultOverGrid(
            std::move(aggregate_values), aggregation_range, stepsInTimeSeriesRange(start_time, end_time, step));

    builder.select_list.push_back(std::move(aggregate_values));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    if (has_group)
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    if (argument.select_query)
    {
        auto & subqueries = context.subqueries;
        subqueries.emplace_back(subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE);
        builder.from_table = subqueries.back().name;
    }

    SQLQueryPiece res = argument;
    res.node = node;
    res.scalar_value = {};

    res.select_query = builder.getSelectQuery();
    res.type = ResultType::INSTANT_VECTOR;
    res.store_method = has_group ? StoreMethod::VECTOR_GRID : StoreMethod::SCALAR_GRID;
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;

    if (has_group && drop_metric_name.value_or(impl_info->drop_metric_name))
        res = dropMetricName(std::move(res), context);

    return res;
}

}
