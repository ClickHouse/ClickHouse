#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getToGridAggregateFunctionArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fixedAtModifier.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    struct ImplInfo
    {
        std::string_view ch_function_name;
        bool drop_metric_name = true;

        /// The aggregate function returns a sample's value (can be Float32), a sample's timestamp (a DateTime type) or a count (UInt64)
        /// instead of Float64, so the result must be cast.
        bool needs_cast_to_float64 = false;

        /// The number of scalar arguments after the range vector. They must be constants and are passed to the aggregate
        /// function as parameters after the grid parameters.
        size_t num_constant_scalar_arguments = 0;
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

            {"double_exponential_smoothing",
             {
                 "timeSeriesDoubleExponentialSmoothingToGrid",
                 /* drop_metric_name = */ true,
                 /* needs_cast_to_float64 = */ false,
                 /* num_constant_scalar_arguments = */ 2,
             }},

            {"stddev_over_time",
             {
                 "timeSeriesStddevToGrid",
                 /* drop_metric_name = */ true,
             }},

            {"stdvar_over_time",
             {
                 "timeSeriesStdvarToGrid",
                 /* drop_metric_name = */ true,
             }},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }

    /// Builds the query calling the aggregate function described by `impl_info` over the range vector `arguments[0]`.
    /// `constant_parameters` are passed to the aggregate function after the grid parameters.
    SQLQueryPiece applyAggregateFunctionOverRange(
        const Node * node,
        const ImplInfo & impl_info,
        std::vector<SQLQueryPiece> && arguments,
        ConverterContext & context,
        bool drop_metric_name,
        ASTs && constant_parameters)
    {
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
        const auto aggregation_range = getRangeAggregationRange(argument, node_range, context);

        /// A range vector made from a scalar has no `group` column. The result is still a vector grid with one series without
        /// tags, because the aggregate function returns NULL for a window without enough samples and only VECTOR_GRID can hold NULL.
        const bool from_scalar = (argument.store_method == StoreMethod::CONST_SCALAR)
            || (argument.store_method == StoreMethod::SINGLE_SCALAR) || (argument.store_method == StoreMethod::SCALAR_GRID);

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

        /// <aggregate_function>(<timestamps>, <values>) AS values
        auto aggregate_function = addParametersToAggregateFunction(
            makeASTFunction(impl_info.ch_function_name, std::move(aggregate_function_arguments)),
            timeSeriesTimestampToAST(aggregation_range.start_time, context.result_timestamp_type),
            timeSeriesTimestampToAST(aggregation_range.end_time, context.result_timestamp_type),
            timeSeriesDurationToAST(aggregation_range.step, context.result_timestamp_type),
            timeSeriesDurationToAST(window, context.result_timestamp_type));

        for (auto & parameter : constant_parameters)
            aggregate_function = addParametersToAggregateFunction(std::move(aggregate_function), std::move(parameter));

        ASTPtr aggregate_values = std::move(aggregate_function);

        if (impl_info.needs_cast_to_float64)
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

        if (!from_scalar)
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
        res.store_method = StoreMethod::VECTOR_GRID;
        res.start_time = start_time;
        res.end_time = end_time;
        res.step = step;

        if (from_scalar)
        {
            /// A scalar has no metric name.
            res.metric_name_dropped = true;
        }
        else if (drop_metric_name)
        {
            res = dropMetricName(std::move(res), context);
        }

        return res;
    }
}


bool isFunctionOverRange(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto * impl_info = getImplInfo(function_node->function_name);
    chassert(impl_info);

    const size_t expected_number_of_arguments = 1 + impl_info->num_constant_scalar_arguments;
    checkNumberOfArguments(function_node, arguments, expected_number_of_arguments);
    checkArgumentType(function_node, arguments, 0, ResultType::RANGE_VECTOR, context);

    /// In PromQL, the other arguments of a function over a range vector are always scalars.
    /// Whether they are constants is checked below, when they are converted to parameters.
    for (size_t i = 1; i < expected_number_of_arguments; ++i)
        checkArgumentType(function_node, arguments, i, ResultType::SCALAR, context);

    ASTs constant_parameters;
    for (size_t i = 1; i < expected_number_of_arguments; ++i)
    {
        const auto & argument = arguments[i];
        if (argument.store_method == StoreMethod::EMPTY)
            return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY}; /// A scalar without a value, so the result is empty too.

        if (argument.store_method != StoreMethod::CONST_SCALAR)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "{} supports only a constant as argument #{}, but got {}",
                            function_node->getTypeAndName(), i + 1, getPromQLText(argument, context));

        constant_parameters.push_back(make_intrusive<ASTLiteral>(argument.scalar_value));
    }

    return applyAggregateFunctionOverRange(
        function_node, *impl_info, std::move(arguments), context, impl_info->drop_metric_name, std::move(constant_parameters));
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

    return applyAggregateFunctionOverRange(
        node, *impl_info, std::move(arguments), context, drop_metric_name.value_or(impl_info->drop_metric_name), {});
}

}
