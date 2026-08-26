#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropHistogramValues.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
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
    /// this node after setEvaluationTime, so the fixed evaluation time is resolved here without hidden state in SQLQueryPiece.
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
        /// The histogram sibling of ch_function_name (empty for functions without one): computes the same PromQL function
        /// over native-histogram samples. `histogram_instant` selects the instant (irate/idelta) kind detection for `sample_kinds`.
        std::string_view ch_histogram_function_name = {};
        bool histogram_instant = false;
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

            /// TODO: predict_linear, avg_over_time, min_over_time, max_over_time, sum_over_time, count_over_time, quantile_over_time,
            /// stddev_over_time, stdvar_over_time, present_over_time, absent_over_time, mad_over_time, first_over_time, ts_of_*_over_time
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }

    ASTPtr makeIdentifier(String name)
    {
        return make_intrusive<ASTIdentifier>(std::move(name));
    }

    ASTPtr makeNullLiteral()
    {
        return make_intrusive<ASTLiteral>(Field{});
    }

    /// arrayMap(lambda(tuple(<arg names...>), <body>), <arrays...>)
    ASTPtr makeArrayMap(std::initializer_list<const char *> arg_names, ASTPtr body, ASTs arrays)
    {
        auto args_tuple = makeASTFunction("tuple");
        for (const char * arg_name : arg_names)
            args_tuple->arguments->children.push_back(make_intrusive<ASTIdentifier>(arg_name));

        auto array_map = makeASTFunction("arrayMap", makeASTFunction("lambda", std::move(args_tuple), std::move(body)));
        for (auto & array : arrays)
            array_map->arguments->children.push_back(std::move(array));
        return array_map;
    }

    /// The `if(equals(k, <kind>), v, NULL)` mask selecting one sample kind of a combined grid.
    ASTPtr makeKindMask(String values_alias, UInt64 kind)
    {
        return makeArrayMap(
            {"v", "k"},
            makeASTFunction("if",
                makeASTFunction("equals", makeIdentifier("k"), make_intrusive<ASTLiteral>(kind)),
                makeIdentifier("v"),
                makeNullLiteral()),
            {makeIdentifier(std::move(values_alias)), makeIdentifier(ColumnNames::SampleKinds)});
    }

    /// Wraps the rate family's grid subquery with the projection deriving `sample_kinds` and masking both arms with it.
    /// Upstream drops a mixed-kind window (NewMixedFloatsHistogramsWarning, instantValue), so at one step at most one arm is non-NULL.
    ASTPtr buildRateFamilyProjection(ASTPtr inner_query, const ImplInfo & impl_info, ConverterContext & context)
    {
        const String floats_present = "floats_present";
        const String histograms_present = "histograms_present";
        const String kinds_delta = "kinds_delta";
        const String newest_kind = "newest_kind";

        ASTPtr values_masked;
        ASTPtr histogram_values_masked;
        ASTPtr sample_kinds;

        if (impl_info.histogram_instant)
        {
            /// values: if the two newest samples are both floats, keep the float arm.
            values_masked = makeArrayMap(
                {"v", "kd", "nk"},
                makeASTFunction("if",
                    makeASTFunction("and",
                        makeASTFunction("equals", makeIdentifier("kd"), make_intrusive<ASTLiteral>(UInt64{0})),
                        makeASTFunction("equals", makeIdentifier("nk"), make_intrusive<ASTLiteral>(UInt64{0}))),
                    makeIdentifier("v"),
                    makeNullLiteral()),
                {makeIdentifier(ColumnNames::Values), makeIdentifier(kinds_delta), makeIdentifier(newest_kind)});

            /// histogram_values: if the two newest samples are both histograms, keep the histogram arm.
            histogram_values_masked = makeArrayMap(
                {"h", "kd", "nk"},
                makeASTFunction("if",
                    makeASTFunction("and",
                        makeASTFunction("equals", makeIdentifier("kd"), make_intrusive<ASTLiteral>(UInt64{0})),
                        makeASTFunction("equals", makeIdentifier("nk"), make_intrusive<ASTLiteral>(UInt64{1}))),
                    makeIdentifier("h"),
                    makeNullLiteral()),
                {makeIdentifier(ColumnNames::HistogramValues), makeIdentifier(kinds_delta), makeIdentifier(newest_kind)});

            /// sample_kinds: NULL when the two newest samples are mixed (or fewer than 2 samples);
            /// the shared kind otherwise.
            sample_kinds = makeArrayMap(
                {"kd", "nk"},
                makeASTFunction("if",
                    makeASTFunction("or",
                        makeASTFunction("isNull", makeIdentifier("kd")),
                        makeASTFunction("notEquals", makeIdentifier("kd"), make_intrusive<ASTLiteral>(UInt64{0}))),
                    makeNullLiteral(),
                    makeIdentifier("nk")),
                {makeIdentifier(kinds_delta), makeIdentifier(newest_kind)});
        }
        else
        {
            auto mixed_condition = [&]
            {
                return makeASTFunction("and",
                    makeASTFunction("isNotNull", makeIdentifier("fp")),
                    makeASTFunction("isNotNull", makeIdentifier("hp")));
            };

            values_masked = makeArrayMap(
                {"v", "fp", "hp"},
                makeASTFunction("if", mixed_condition(), makeNullLiteral(), makeIdentifier("v")),
                {makeIdentifier(ColumnNames::Values), makeIdentifier(floats_present), makeIdentifier(histograms_present)});

            histogram_values_masked = makeArrayMap(
                {"h", "fp", "hp"},
                makeASTFunction("if", mixed_condition(), makeNullLiteral(), makeIdentifier("h")),
                {makeIdentifier(ColumnNames::HistogramValues), makeIdentifier(floats_present), makeIdentifier(histograms_present)});

            /// sample_kinds: NULL for a mixed window; otherwise the kind of the arm that produced a
            /// result (NULL when neither did).
            sample_kinds = makeArrayMap(
                {"v", "h", "fp", "hp"},
                makeASTFunction("multiIf",
                    mixed_condition(), makeNullLiteral(),
                    makeASTFunction("isNotNull", makeIdentifier("v")), make_intrusive<ASTLiteral>(Float64{0}),
                    makeASTFunction("isNotNull", makeIdentifier("h")), make_intrusive<ASTLiteral>(Float64{1}),
                    makeNullLiteral()),
                {makeIdentifier(ColumnNames::Values), makeIdentifier(ColumnNames::HistogramValues),
                 makeIdentifier(floats_present), makeIdentifier(histograms_present)});
        }

        SelectQueryBuilder builder;
        builder.select_list.push_back(makeIdentifier(ColumnNames::Group));
        builder.select_list.push_back(std::move(values_masked));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        builder.select_list.push_back(std::move(histogram_values_masked));
        builder.select_list.back()->setAlias(ColumnNames::HistogramValues);
        builder.select_list.push_back(std::move(sample_kinds));
        builder.select_list.back()->setAlias(ColumnNames::SampleKinds);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(inner_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

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
    ASTPtr float_if_condition;
    boost::intrusive_ptr<ASTFunction> histogram_values;
    boost::intrusive_ptr<ASTFunction> sample_kinds;
    /// The rate family: the kind-helper aggregates (alias, aggregate) that the outer projection
    /// (buildRateFamilyProjection) turns into `sample_kinds`.
    std::vector<std::pair<String, boost::intrusive_ptr<ASTFunction>>> sample_kinds_helpers;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        {
            /// SELECT <aggregate_function>(timeSeriesRange(...), arrayResize([], <count_of_time_steps>, <scalar_value>)) AS values
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
            /// SELECT <aggregate_function>(timeSeriesRange(<start_time>, <end_time>, <step>), values) AS values
            /// FROM <scalar_grid>
            values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT group, <aggregate_function>((timeSeriesFromGrid(...) AS time_series).1, time_series.2) AS values
            /// FROM <vector_grid> GROUP BY group
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
            /// SELECT group, <aggregate_function>(timestamp, value) AS values
            /// FROM <raw_data> GROUP BY group
            has_group = true;

            timestamps = make_intrusive<ASTIdentifier>(ColumnNames::Timestamp);
            values = make_intrusive<ASTIdentifier>(ColumnNames::Value);
            res.store_method = StoreMethod::VECTOR_GRID;

            break;
        }

        case StoreMethod::HISTOGRAM_RAW_DATA:
        {
            /// Rate family / last_over_time: per-kind `-If` aggregates for both arms plus `sample_kinds` helper aggregates.
            /// Other functions drop histograms (Prometheus semantics for non-histogram functions): a float-only `-If` aggregate.
            has_group = true;

            timestamps = make_intrusive<ASTIdentifier>(ColumnNames::Timestamp);
            values = make_intrusive<ASTIdentifier>(ColumnNames::Value);
            res.store_method = StoreMethod::VECTOR_GRID;

            /// The float aggregate consumes only the float samples via the -If combinator.
            float_if_condition = makeASTFunction(
                "equals", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram), make_intrusive<ASTLiteral>(UInt64{0}));

            if (!impl_info->ch_histogram_function_name.empty())
            {
                /// The histogram aggregate consumes only the histogram samples; it gets the same grid
                /// parameters as the float aggregate, so `values` and `histogram_values` stay equal-length.
                ASTs payload_columns;
                for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
                    payload_columns.push_back(make_intrusive<ASTIdentifier>(name));

                auto payload_tuple = makeASTFunction("tuple");
                payload_tuple->arguments->children = std::move(payload_columns);

                histogram_values = makeASTFunction(
                    String(impl_info->ch_histogram_function_name) + "If",
                    make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                    std::move(payload_tuple),
                    makeASTFunction("equals", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram), make_intrusive<ASTLiteral>(UInt64{1})));

                auto kind_flags = []
                {
                    return makeASTFunction("toFloat64", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram));
                };

                if (impl_info->histogram_instant)
                {
                    /// irate/idelta (upstream's `instantValue`): a mixed newest pair drops the element; `kinds_delta` is the
                    /// idelta over the kind flags (non-NULL and 0 iff the newest two share a kind), `newest_kind` is that kind.
                    sample_kinds_helpers.emplace_back("kinds_delta",
                        makeASTFunction("timeSeriesInstantDeltaToGrid",
                            make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                            kind_flags()));
                    sample_kinds_helpers.emplace_back("newest_kind",
                        makeASTFunction("timeSeriesLastToGrid",
                            make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                            kind_flags()));
                }
                else
                {
                    /// rate/increase/delta (upstream's `extrapolatedRate`): a mixed-kind window drops the element;
                    /// each kind's presence probe is the non-NULL-ness of its last-over-time resample.
                    sample_kinds_helpers.emplace_back("floats_present",
                        makeASTFunction("timeSeriesLastToGridIf",
                            make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                            make_intrusive<ASTIdentifier>(ColumnNames::Value),
                            makeASTFunction("equals", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram), make_intrusive<ASTLiteral>(UInt64{0}))));
                    sample_kinds_helpers.emplace_back("histograms_present",
                        makeASTFunction("timeSeriesLastToGridIf",
                            make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                            kind_flags(),
                            makeASTFunction("equals", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram), make_intrusive<ASTLiteral>(UInt64{1}))));
                }

                res.store_method = StoreMethod::HISTOGRAM_GRID;
            }
            else if (function_name == "last_over_time")
            {
                ASTs payload_columns;
                for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
                    payload_columns.push_back(make_intrusive<ASTIdentifier>(name));

                auto payload_tuple = makeASTFunction("tuple");
                payload_tuple->arguments->children = std::move(payload_columns);

                /// The histogram aggregate consumes only the histogram samples; it gets the same
                /// grid parameters as the float aggregate, so `values` and `histogram_values` stay equal-length.
                histogram_values = makeASTFunction(
                    "timeSeriesHistogramLastToGridIf",
                    make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                    std::move(payload_tuple),
                    makeASTFunction("equals", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram), make_intrusive<ASTLiteral>(UInt64{1})));

                /// Precedence oracle: the kind (0=float, 1=histogram) of the newest sample of either type in the window, NULL if empty.
                /// A source-timestamp tie keeps the histogram, see AggregateFunctionTimeseriesToGridSparse::Summary::add.
                sample_kinds = makeASTFunction(
                    impl_info->ch_function_name,
                    make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                    makeASTFunction("toFloat64", make_intrusive<ASTIdentifier>(ColumnNames::IsHistogram)));

                res.store_method = StoreMethod::HISTOGRAM_GRID;
            }

            break;
        }

        case StoreMethod::HISTOGRAM_GRID:
        {
            /// Range-vector functions compute new float values from `values`, dropping the histogram payloads of a combined grid
            /// (see dropHistogramValues); `last_over_time` and the rate family are the histogram-preserving exceptions.
            if (impl_info->ch_histogram_function_name.empty() && function_name != "last_over_time")
                return applyFunctionOverRange(node, function_name, {dropHistogramValues(std::move(argument), context)}, context);

            if (!impl_info->ch_histogram_function_name.empty())
            {
                /// The rate family over a combined grid (e.g. a subquery): the inner grid resolved one sample kind per step,
                /// so each arm is resampled masked by `sample_kinds`, and the kind helpers resample the `sample_kinds` series itself.
                has_group = true;
                res.store_method = StoreMethod::HISTOGRAM_GRID;

                /// (timeSeriesFromGrid(<inner>, arrayMap((v, k) -> if(k = 0, v, NULL), values, sample_kinds)) AS time_series).1
                ASTPtr float_series = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                    makeKindMask(ColumnNames::Values, 0));
                float_series->setAlias(ColumnNames::TimeSeries);
                timestamps = makeASTFunction("tupleElement", std::move(float_series), make_intrusive<ASTLiteral>(1));

                /// time_series.2
                values = makeASTFunction(
                    "tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries), make_intrusive<ASTLiteral>(2));

                /// The -Array combinator is required: the histogram aggregate's creator rejects array arguments, so it strips
                /// the arrays and drives the per-element scalar add; shared `timeSeriesFromGrid` offsets satisfy its alignment requirement.
                ASTPtr histogram_series = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                    makeKindMask(ColumnNames::HistogramValues, 1));
                histogram_series->setAlias(ColumnNames::HistogramTimeSeries);
                histogram_values = makeASTFunction(
                    String(impl_info->ch_histogram_function_name) + "Array",
                    makeASTFunction("tupleElement", std::move(histogram_series), make_intrusive<ASTLiteral>(1)),
                    makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::HistogramTimeSeries), make_intrusive<ASTLiteral>(2)));

                /// (timeSeriesFromGrid(<inner>, sample_kinds) AS sample_kinds_time_series).1/.2
                ASTPtr kinds_series = makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                    make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds));
                kinds_series->setAlias(ColumnNames::SampleKindsTimeSeries);
                /// The aliased series node is embedded into the first helper's arguments; later
                /// references go by alias name.
                auto kinds_timestamps = [&, kinds_series_node = std::move(kinds_series)]() mutable -> ASTPtr
                {
                    if (kinds_series_node)
                        return makeASTFunction("tupleElement", std::move(kinds_series_node), make_intrusive<ASTLiteral>(1));
                    return makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::SampleKindsTimeSeries), make_intrusive<ASTLiteral>(1));
                };
                auto kinds_values = [&]
                {
                    return makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::SampleKindsTimeSeries), make_intrusive<ASTLiteral>(2));
                };

                if (impl_info->histogram_instant)
                {
                    /// The same helpers as in the HISTOGRAM_RAW_DATA arm, over the resampled kinds.
                    sample_kinds_helpers.emplace_back("kinds_delta",
                        makeASTFunction("timeSeriesInstantDeltaToGrid", kinds_timestamps(), kinds_values()));
                    sample_kinds_helpers.emplace_back("newest_kind",
                        makeASTFunction("timeSeriesLastToGrid", kinds_timestamps(), kinds_values()));
                }
                else
                {
                    /// floats_present / histograms_present: a window holds a value iff an inner step resolved to that kind.
                    /// -If cannot be used: the resampled arguments are arrays, and the -If condition is per-row, not per-element.
                    auto presence_probe = [&](const String & alias, UInt64 kind)
                    {
                        ASTPtr series = makeASTFunction(
                            "timeSeriesFromGrid",
                            timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                            timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                            timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                            makeKindMask(ColumnNames::SampleKinds, kind));
                        series->setAlias(alias);
                        return makeASTFunction(
                            "timeSeriesLastToGrid",
                            makeASTFunction("tupleElement", std::move(series), make_intrusive<ASTLiteral>(1)),
                            makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(alias), make_intrusive<ASTLiteral>(2)));
                    };
                    sample_kinds_helpers.emplace_back("floats_present", presence_probe("floats_present_series", 0));
                    sample_kinds_helpers.emplace_back("histograms_present", presence_probe("histograms_present_series", 1));
                }

                break;
            }

            /// The combined grid lives on the argument's own range and step (e.g. a subquery's inner grid), so `timeSeriesFromGrid`
            /// reconstructs each arm's series (skipping NULLs), and last-over-time aggregates resample it onto the aggregation grid.
            has_group = true;
            res.store_method = StoreMethod::HISTOGRAM_GRID;

            /// (timeSeriesFromGrid(<inner>, values) AS time_series).1
            ASTPtr float_series = makeASTFunction(
                "timeSeriesFromGrid",
                timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                make_intrusive<ASTIdentifier>(ColumnNames::Values));
            float_series->setAlias(ColumnNames::TimeSeries);
            timestamps = makeASTFunction("tupleElement", std::move(float_series), make_intrusive<ASTLiteral>(1));

            /// time_series.2
            values = makeASTFunction(
                "tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries), make_intrusive<ASTLiteral>(2));

            /// The -Array combinator is required here: the histogram aggregate's creator rejects array arguments; it strips the arrays
            /// and drives the scalar add per element; the shared `timeSeriesFromGrid` offsets satisfy the alignment requirement.
            ASTPtr histogram_series = makeASTFunction(
                "timeSeriesFromGrid",
                timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues));
            histogram_series->setAlias(ColumnNames::HistogramTimeSeries);
            histogram_values = makeASTFunction(
                "timeSeriesHistogramLastToGridArray",
                makeASTFunction("tupleElement", std::move(histogram_series), make_intrusive<ASTLiteral>(1)),
                makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::HistogramTimeSeries), make_intrusive<ASTLiteral>(2)));

            /// timeSeriesLastToGrid((timeSeriesFromGrid(<inner>, sample_kinds) AS sample_kinds_time_series).1, sample_kinds_time_series.2)
            ASTPtr kinds_series = makeASTFunction(
                "timeSeriesFromGrid",
                timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds));
            kinds_series->setAlias(ColumnNames::SampleKindsTimeSeries);
            sample_kinds = makeASTFunction(
                impl_info->ch_function_name,
                makeASTFunction("tupleElement", std::move(kinds_series), make_intrusive<ASTLiteral>(1)),
                makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::SampleKindsTimeSeries), make_intrusive<ASTLiteral>(2)));

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

    String ch_function_name{impl_info->ch_function_name};
    ASTs aggregate_arguments;
    aggregate_arguments.push_back(std::move(timestamps));
    aggregate_arguments.push_back(std::move(values));
    if (float_if_condition)
    {
        ch_function_name += "If";
        aggregate_arguments.push_back(std::move(float_if_condition));
    }

    auto float_aggregate = makeASTFunction(ch_function_name);
    float_aggregate->arguments->children = std::move(aggregate_arguments);

    /// Adds the grid parameters (start, end, step, window) to an aggregate function; for a fixed @ modifier the result
    /// is evaluated once (Prometheus semantics), so it is repeated across the outer grid via `arrayResize` instead of sliding.
    auto add_grid_parameters = [&](boost::intrusive_ptr<ASTFunction> aggregate)
    {
        aggregate = addParametersToAggregateFunction(
            std::move(aggregate),
            timeSeriesTimestampToAST(aggregation_start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(aggregation_end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(aggregation_step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type));

        if (fixed_at_node)
        {
            aggregate = makeASTFunction(
                "arrayResize",
                make_intrusive<ASTLiteral>(Array{}),
                make_intrusive<ASTLiteral>(result_grid_size),
                makeASTFunction(
                    "arrayElement", std::move(aggregate), make_intrusive<ASTLiteral>(aggregation_grid_size)));
        }

        return aggregate;
    };

    /// <aggregate_function>(<timestamps>, <values>) AS values
    builder.select_list.push_back(add_grid_parameters(std::move(float_aggregate)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    if (histogram_values)
    {
        /// <histogram aggregate>(...) AS histogram_values
        builder.select_list.push_back(add_grid_parameters(std::move(histogram_values)));
        builder.select_list.back()->setAlias(ColumnNames::HistogramValues);

        if (sample_kinds)
        {
            /// last_over_time: timeSeriesLastToGrid(...)(timestamp, toFloat64(is_histogram)) AS sample_kinds
            builder.select_list.push_back(add_grid_parameters(std::move(sample_kinds)));
            builder.select_list.back()->setAlias(ColumnNames::SampleKinds);
        }

        /// The rate family: the kind helpers consumed by the outer projection (buildRateFamilyProjection).
        for (auto & [alias, helper] : sample_kinds_helpers)
        {
            builder.select_list.push_back(add_grid_parameters(std::move(helper)));
            builder.select_list.back()->setAlias(alias);
        }
    }

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

    /// The rate family: wrap the grid subquery with the projection deriving `sample_kinds` from the arms and helpers
    /// and masking both arms with it (upstream drops mixed-kind windows).
    if (!sample_kinds_helpers.empty())
        res.select_query = buildRateFamilyProjection(std::move(res.select_query), *impl_info, context);

    if (has_group && impl_info->drop_metric_name)
        res = dropMetricName(std::move(res), context);

    return res;
}

}
