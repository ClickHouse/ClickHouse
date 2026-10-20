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

    /// The empty-name marker is a pending metric-name drop. It is not part of the series identity.
    ASTPtr groupWithoutDroppedMetricNameMarker()
    {
        return makeASTFunction(
            "timeSeriesRemoveTag",
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            make_intrusive<ASTLiteral>(kDroppedMetricNameMarker));
    }

    /// True when two grids of one series both have a sample at the same point.
    ASTPtr gridsOverlap()
    {
        return makeASTFunction(
            "arrayExists",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                makeASTFunction("greater", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(1u))),
            makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
    }

    /// 1 where this row carries the pending drop, 0 where it does not, NULL where the grid has no sample.
    ASTPtr markerGridAlignedToValues()
    {
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                makeASTFunction(
                    "if",
                    makeASTFunction("isNull", make_intrusive<ASTIdentifier>("x")),
                    make_intrusive<ASTLiteral>(Field{}),
                    makeASTFunction(
                        "if",
                        makeASTFunction(
                            "isNotNull",
                            makeASTFunction(
                                "timeSeriesExtractTag",
                                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                                make_intrusive<ASTLiteral>(kDroppedMetricNameMarker))),
                        make_intrusive<ASTLiteral>(1.0),
                        make_intrusive<ASTLiteral>(0.0)))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));
    }

    /// Samples for the same window function, read from the marker grid instead of the values.
    ASTs markerSamplesFromGrid(const SQLQueryPiece & range_vector, ConverterContext & context)
    {
        ASTPtr marker_samples = makeASTFunction(
            "timeSeriesFromGrid",
            timeSeriesTimestampToAST(range_vector.start_time, context.result_timestamp_type),
            timeSeriesTimestampToAST(range_vector.end_time, context.result_timestamp_type),
            timeSeriesDurationToAST(range_vector.step, context.result_timestamp_type),
            markerGridAlignedToValues());
        marker_samples->setAlias("marker_samples");

        ASTPtr timestamps = makeASTFunction("tupleElement", std::move(marker_samples), make_intrusive<ASTLiteral>(1));
        ASTPtr values = makeASTFunction(
            "tupleElement", make_intrusive<ASTIdentifier>("marker_samples"), make_intrusive<ASTLiteral>(2));
        return {std::move(timestamps), std::move(values)};
    }

    ASTPtr markerEquals(Float64 expected)
    {
        return makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("m")),
            makeASTFunction(
                "and",
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("m")),
                makeASTFunction(
                    "equals",
                    makeASTFunction("ifNull", make_intrusive<ASTIdentifier>("m"), make_intrusive<ASTLiteral>(0.0)),
                    make_intrusive<ASTLiteral>(expected))));
    }

    ASTPtr uint64Array(UInt64 first)
    {
        return makeASTFunction("array", make_intrusive<ASTLiteral>(first));
    }

    ASTPtr uint64Array(UInt64 first, UInt64 second)
    {
        return makeASTFunction("array", make_intrusive<ASTLiteral>(first), make_intrusive<ASTLiteral>(second));
    }

    /// `first_over_time` and `last_over_time` keep the metric name of the sample they return.
    /// `marker_grid` is that function applied to the pending-drop flags, so each output point
    /// keeps the flag of its own sample. An empty window does not gain the marker.
    ASTPtr gridWithMarkerOfSelectedSample(ASTPtr aggregation_query, ConverterContext & context)
    {
        constexpr const char * marker_grid = "marker_grid";
        constexpr const char * marker_side = "marker_side";

        SelectQueryBuilder sides;
        context.subqueries.emplace_back(context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE);
        sides.from_table = context.subqueries.back().name;
        sides.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        sides.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewValues));
        sides.select_list.push_back(make_intrusive<ASTIdentifier>(marker_grid));

        /// Side 1 carries the pending drop. Side 0 keeps the metric name.
        /// A side is present only when a selected sample has that flag. An empty window stays on side 0.
        ASTPtr has_marker = makeASTFunction(
            "arrayExists", markerEquals(1.0), make_intrusive<ASTIdentifier>(marker_grid));
        ASTPtr lacks_marker = makeASTFunction(
            "arrayExists", markerEquals(0.0), make_intrusive<ASTIdentifier>(marker_grid));
        sides.select_list.push_back(makeASTFunction(
            "arrayJoin",
            makeASTFunction(
                "if",
                std::move(has_marker),
                makeASTFunction("if", std::move(lacks_marker), uint64Array(0, 1), uint64Array(1)),
                uint64Array(0))));
        sides.select_list.back()->setAlias(marker_side);

        context.subqueries.emplace_back(context.subqueries.size(), sides.getSelectQuery(), SQLSubqueryType::TABLE);

        SelectQueryBuilder builder;
        builder.from_table = context.subqueries.back().name;

        ASTPtr marked_group = makeASTFunction(
            "timeSeriesReplaceTag",
            make_intrusive<ASTIdentifier>(ColumnNames::NewGroup),
            make_intrusive<ASTLiteral>(kDroppedMetricNameMarker),
            make_intrusive<ASTLiteral>(kDroppedMetricNameMarkerValue),
            make_intrusive<ASTLiteral>(String{}),
            make_intrusive<ASTLiteral>(String{}));
        builder.select_list.push_back(makeASTFunction(
            "if",
            makeASTFunction("equals", make_intrusive<ASTIdentifier>(marker_side), make_intrusive<ASTLiteral>(1u)),
            std::move(marked_group),
            make_intrusive<ASTIdentifier>(ColumnNames::NewGroup)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        ASTPtr value_on_marked_side = makeASTFunction(
            "if",
            makeASTFunction(
                "and",
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("m")),
                makeASTFunction(
                    "equals",
                    makeASTFunction("ifNull", make_intrusive<ASTIdentifier>("m"), make_intrusive<ASTLiteral>(0.0)),
                    make_intrusive<ASTLiteral>(1.0))),
            make_intrusive<ASTIdentifier>("v"),
            make_intrusive<ASTLiteral>(Field{}));
        ASTPtr value_on_unmarked_side = makeASTFunction(
            "if",
            makeASTFunction(
                "or",
                makeASTFunction("isNull", make_intrusive<ASTIdentifier>("m")),
                makeASTFunction(
                    "equals",
                    makeASTFunction("ifNull", make_intrusive<ASTIdentifier>("m"), make_intrusive<ASTLiteral>(0.0)),
                    make_intrusive<ASTLiteral>(0.0))),
            make_intrusive<ASTIdentifier>("v"),
            make_intrusive<ASTLiteral>(Field{}));
        builder.select_list.push_back(makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("v"), make_intrusive<ASTIdentifier>("m")),
                makeASTFunction(
                    "if",
                    makeASTFunction("equals", make_intrusive<ASTIdentifier>(marker_side), make_intrusive<ASTLiteral>(1u)),
                    std::move(value_on_marked_side),
                    std::move(value_on_unmarked_side))),
            make_intrusive<ASTIdentifier>(ColumnNames::NewValues),
            make_intrusive<ASTIdentifier>(marker_grid)));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        return builder.getSelectQuery();
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

    /// A range vector made from a scalar has no `group` column. The result is still a vector grid with one series without
    /// tags, because the aggregate function returns NULL for a window without enough samples and only VECTOR_GRID can hold NULL.
    const bool from_scalar = (argument.store_method != StoreMethod::VECTOR_GRID) && (argument.store_method != StoreMethod::RAW_DATA);

    /// A subquery stores one grid per group. The pending drop can differ between points of one series,
    /// so those grids share the original labels, including `__name__`, and are merged before the window.
    const bool merge_vector_grids = argument.store_method == StoreMethod::VECTOR_GRID;
    const bool drops_metric_name = drop_metric_name.value_or(impl_info->drop_metric_name);
    /// `first_over_time` and `last_over_time` keep the metric name of the sample they return.
    /// The pending drop of that sample is stored beside the merge key and applied to the output group.
    const bool track_selected_sample_marker = merge_vector_grids && !drops_metric_name
        && (function_name == "last_over_time" || function_name == "first_over_time");

    auto wrap_grid_aggregate = [&](ASTPtr aggregate) -> ASTPtr
    {
        if (impl_info->needs_cast_to_float64)
        {
            /// CAST(<aggregate_function>(timestamp, value), 'Array(Nullable(Float64))')
            /// See `needs_cast_to_float64`; a timestamp becomes seconds since 1970-01-01 as in Prometheus. The cast does nothing
            /// for Float64 and is cheap anyway: the aggregated grid is much smaller than the raw data.
            aggregate = makeASTFunction("CAST", std::move(aggregate), make_intrusive<ASTLiteral>("Array(Nullable(Float64))"));
        }

        if (fixed_at_node)
            aggregate = repeatFixedAtResultOverGrid(
                std::move(aggregate), aggregation_range, stepsInTimeSeriesRange(start_time, end_time, step));
        return aggregate;
    };

    SelectQueryBuilder builder;

    if (from_scalar)
    {
        builder.select_list.push_back(timeSeriesNoTagsGroupToAST());
        builder.select_list.back()->setAlias(ColumnNames::Group);
    }
    else if (merge_vector_grids)
    {
        auto merge_key = groupWithoutDroppedMetricNameMarker();
        merge_key->setAlias(ColumnNames::NewGroup);
        builder.select_list.push_back(std::move(merge_key));
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        builder.having = makeASTFunction(
            "equals",
            makeASTFunction(
                "timeSeriesThrowDuplicateSeriesIf",
                gridsOverlap(),
                make_intrusive<ASTIdentifier>(ColumnNames::NewGroup)),
            make_intrusive<ASTLiteral>(0u));
    }
    else
    {
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    }

    /// <aggregate_function>(<timestamps>, <values>) AS values
    ASTPtr aggregate_values = wrap_grid_aggregate(addParametersToAggregateFunction(
        makeASTFunction(impl_info->ch_function_name, std::move(aggregate_function_arguments)),
        timeSeriesTimestampToAST(aggregation_range.start_time, context.result_timestamp_type),
        timeSeriesTimestampToAST(aggregation_range.end_time, context.result_timestamp_type),
        timeSeriesDurationToAST(aggregation_range.step, context.result_timestamp_type),
        timeSeriesDurationToAST(window, context.result_timestamp_type)));

    builder.select_list.push_back(std::move(aggregate_values));
    builder.select_list.back()->setAlias(merge_vector_grids ? ColumnNames::NewValues : ColumnNames::Values);

    if (track_selected_sample_marker)
    {
        ASTPtr marker_values = wrap_grid_aggregate(addParametersToAggregateFunction(
            makeASTFunction(impl_info->ch_function_name, markerSamplesFromGrid(argument, context)),
            timeSeriesTimestampToAST(aggregation_range.start_time, context.result_timestamp_type),
            timeSeriesTimestampToAST(aggregation_range.end_time, context.result_timestamp_type),
            timeSeriesDurationToAST(aggregation_range.step, context.result_timestamp_type),
            timeSeriesDurationToAST(window, context.result_timestamp_type)));
        builder.select_list.push_back(std::move(marker_values));
        builder.select_list.back()->setAlias("marker_grid");
    }

    if (argument.select_query)
    {
        auto & subqueries = context.subqueries;
        subqueries.emplace_back(subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE);
        builder.from_table = subqueries.back().name;
    }

    SQLQueryPiece res = argument;
    res.node = node;
    res.scalar_value = {};

    if (track_selected_sample_marker)
        res.select_query = gridWithMarkerOfSelectedSample(builder.getSelectQuery(), context);
    else if (merge_vector_grids)
    {
        SelectQueryBuilder rename;
        context.subqueries.emplace_back(context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE);
        rename.from_table = context.subqueries.back().name;
        rename.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        rename.select_list.back()->setAlias(ColumnNames::Group);
        rename.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewValues));
        rename.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = rename.getSelectQuery();
    }
    else
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
    else if (drops_metric_name)
    {
        /// The merge key does not carry the marker. This function drops the metric name of every series.
        if (merge_vector_grids)
            res.metric_name_dropped = false;
        res = dropMetricName(std::move(res), context);
    }

    return res;
}

}
