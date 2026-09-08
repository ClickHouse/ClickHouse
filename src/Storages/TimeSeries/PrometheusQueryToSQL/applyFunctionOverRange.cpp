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

    /// Returns the fixed @ modifier directly applied to a range-vector argument, if any. Range-vector pieces keep
    /// this node after setEvaluationTime(), so the fixed evaluation time can be resolved here without adding hidden
    /// state to SQLQueryPiece.
    const PrometheusQueryTree::Offset * getFixedAtModifier(const SQLQueryPiece & argument)
    {
        if (argument.type != ResultType::RANGE_VECTOR || !argument.node || argument.node->node_type != NodeType::Offset)
            return nullptr;

        const auto * offset_node = static_cast<const PrometheusQueryTree::Offset *>(argument.node);
        return offset_node->hasAtModifier() ? offset_node : nullptr;
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
        bool drop_metric_name = true;
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

            {"last_over_time",
             {
                 "timeSeriesLastToGrid",
                 /* drop_metric_name = */ false,
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
             }},

            {"resets",
             {
                 "timeSeriesResetsToGrid",
                 /* drop_metric_name = */ true,
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
             }},

            /// TODO:
            /// predict_linear
            /// min_over_time
            /// max_over_time
            /// quantile_over_time
            /// stddev_over_time"
            /// stdvar_over_time
            /// present_over_time
            /// absent_over_time
            /// mad_over_time
            /// ts_of_min_over_time
            /// ts_of_max_over_time
            /// ts_of_last_over_time
            /// first_over_time
            /// ts_of_first_over_time
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
    ConverterContext & context)
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

    const auto * fixed_at_node = getFixedAtModifier(argument);
    auto aggregation_start_time = start_time;
    auto aggregation_end_time = end_time;
    auto aggregation_step = step;

    if (fixed_at_node)
    {
        /// Under a fixed @ modifier the range function is evaluated once at the fixed timestamp, while the
        /// range-vector argument retains its own inner grid.
        const auto & fixed_range = context.node_range_getter.get(fixed_at_node->getExpression());
        chassert(fixed_range.start_time == fixed_range.end_time);
        aggregation_start_time = fixed_range.start_time;
        aggregation_end_time = fixed_range.end_time;
        aggregation_step = DurationType{0};
    }

    SQLQueryPiece res = argument;
    res.node = node;
    res.type = ResultType::INSTANT_VECTOR;

    const auto aggregation_grid_size = stepsInTimeSeriesRange(
        aggregation_start_time, aggregation_end_time, aggregation_step);
    const auto result_grid_size = stepsInTimeSeriesRange(start_time, end_time, step);

    bool has_group = false;
    ASTPtr timestamps;
    ASTPtr values;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        {
            /// SELECT <aggregate_function>(timeSeriesRange(<start_time>, <end_time>, <step>),
            ///                             arrayResize([], <count_of_time_steps>, <scalar_value>)) AS values
            /// FROM <subquery>
            ASTPtr value = (argument.store_method == StoreMethod::CONST_SCALAR)
                ? timeSeriesScalarToAST(argument.scalar_value, context.scalar_data_type)
                : make_intrusive<ASTIdentifier>(ColumnNames::Value);

            /// arrayResize([], <count_of_time_steps>, <scalar_value>)
            values = makeASTFunction(
                "arrayResize",
                make_intrusive<ASTLiteral>(Array{}),
                make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(argument.start_time, argument.end_time, argument.step)),
                value);

            res.store_method = StoreMethod::SCALAR_GRID;
            res.scalar_value = {};
            break;
        }

        case StoreMethod::SCALAR_GRID:
        {
            /// SELECT <aggregate_function>(timeSeriesRange(<start_time>, <end_time>, <step>),
            ///                             values)) AS values
            /// FROM <scalar_grid>
            values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT group,
            ///        <aggregate_function>((timeSeriesFromGrid(<start_time>, <end_time>, <step>, values) AS time_series).1,
            ///                             time_series.2)) AS values
            /// FROM <vector_grid>
            /// GROUP BY group
            has_group = true;

            /// (timeSeriesFromGrid(<start_time>, <end_time>, <step>, values) AS time_series).1
            ASTPtr ts = makeASTFunction(
                "timeSeriesFromGrid",
                timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                make_intrusive<ASTIdentifier>(ColumnNames::Values));
            ts->setAlias(ColumnNames::TimeSeries);
            timestamps = makeASTFunction("tupleElement", std::move(ts), make_intrusive<ASTLiteral>(1));

            /// time_series.2
            values = makeASTFunction(
                "tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries), make_intrusive<ASTLiteral>(2));

            break;
        }

        case StoreMethod::RAW_DATA:
        {
            /// SELECT group,
            ///        <aggregate_function>(timestamp, value) AS values
            /// FROM <raw_data>
            /// GROUP BY group
            has_group = true;

            timestamps = make_intrusive<ASTIdentifier>(ColumnNames::Timestamp);
            values = make_intrusive<ASTIdentifier>(ColumnNames::Value);
            res.store_method = StoreMethod::VECTOR_GRID;

            break;
        }

        case StoreMethod::CONST_STRING:
        {
            /// Can't get in here because the store method CONST_STRING is incompatible with the allowed
            /// argument types (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    chassert(values);

    if (!timestamps)
    {
        /// timeSeriesRange(<start_time>, <end_time>, <step>)
        timestamps = makeASTFunction(
            "timeSeriesRange",
            timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(argument.step, context.timestamp_data_type));
    }

    SelectQueryBuilder builder;

    if (has_group)
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    /// <aggregate_function>(<timestamps>, <values>) AS values
    auto aggregate_values = addParametersToAggregateFunction(
        makeASTFunction(impl_info->ch_function_name, std::move(timestamps), std::move(values)),
        timeSeriesTimestampToAST(aggregation_start_time, context.timestamp_data_type),
        timeSeriesTimestampToAST(aggregation_end_time, context.timestamp_data_type),
        timeSeriesDurationToAST(aggregation_step, context.timestamp_data_type),
        timeSeriesDurationToAST(window, context.timestamp_data_type));

    if (fixed_at_node)
    {
        /// A fixed @ expression is evaluated once by Prometheus. Repeat the single aggregate result on the outer
        /// query grid instead of sliding the range function over the outer evaluation timestamps.
        aggregate_values = makeASTFunction(
            "arrayResize",
            make_intrusive<ASTLiteral>(Array{}),
            make_intrusive<ASTLiteral>(result_grid_size),
            makeASTFunction(
                "arrayElement", std::move(aggregate_values), make_intrusive<ASTLiteral>(aggregation_grid_size)));
    }

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

    res.select_query = builder.getSelectQuery();
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;

    if (has_group && impl_info->drop_metric_name)
        res = dropMetricName(std::move(res), context);

    return res;
}

}
