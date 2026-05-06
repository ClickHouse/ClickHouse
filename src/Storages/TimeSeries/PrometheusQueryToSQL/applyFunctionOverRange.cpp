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

    enum class SimpleOverTimeFunction
    {
        None,
        Sum,
        Avg,
        Min,
        Max,
        Count,
        Stddev,
        Stdvar,
        Present,
    };

    struct ImplInfo
    {
        std::string_view ch_function_name;
        bool drop_metric_name = true;
        SimpleOverTimeFunction simple_over_time_function = SimpleOverTimeFunction::None;
    };

    ASTPtr toFloat64(ASTPtr && x)
    {
        return makeASTFunction("toFloat64", std::move(x));
    }

    ASTPtr getTupleElement(ASTPtr && tuple, UInt64 index)
    {
        return makeASTFunction("tupleElement", std::move(tuple), make_intrusive<ASTLiteral>(index));
    }

    ASTPtr buildWindowSamples(ASTPtr grid_timestamp, ASTPtr timestamps, ASTPtr values, ASTPtr window)
    {
        auto sample_timestamp = [] { return getTupleElement(make_intrusive<ASTIdentifier>("p"), 1); };
        auto sample_value = [] { return getTupleElement(make_intrusive<ASTIdentifier>("p"), 2); };

        auto filter_condition = makeASTFunction(
            "and",
            makeASTFunction(
                "greater",
                toFloat64(sample_timestamp()),
                makeASTFunction("minus", toFloat64(grid_timestamp->clone()), toFloat64(window->clone()))),
            makeASTFunction("lessOrEquals", toFloat64(sample_timestamp()), toFloat64(std::move(grid_timestamp))),
            makeASTFunction("isNotNull", sample_value()));

        auto filtered_samples = makeASTFunction(
            "arrayFilter",
            makeASTLambda({"p"}, std::move(filter_condition)),
            makeASTFunction("arrayZip", std::move(timestamps), std::move(values)));

        return makeASTFunction(
            "arrayMap",
            makeASTLambda({"p"}, makeASTFunction("assumeNotNull", sample_value())),
            std::move(filtered_samples));
    }

    ASTPtr buildSimpleOverTimeValue(SimpleOverTimeFunction function, ASTPtr samples, const ConverterContext & context)
    {
        switch (function)
        {
            case SimpleOverTimeFunction::Sum:
                return makeASTFunction("arraySum", std::move(samples));
            case SimpleOverTimeFunction::Avg:
                return makeASTFunction("arrayAvg", std::move(samples));
            case SimpleOverTimeFunction::Min:
                return makeASTFunction("arrayMin", std::move(samples));
            case SimpleOverTimeFunction::Max:
                return makeASTFunction("arrayMax", std::move(samples));
            case SimpleOverTimeFunction::Count:
                return makeASTFunction("toFloat64", makeASTFunction("length", std::move(samples)));
            case SimpleOverTimeFunction::Stddev:
                return makeASTFunction("arrayReduce", make_intrusive<ASTLiteral>("stddevPop"), std::move(samples));
            case SimpleOverTimeFunction::Stdvar:
                return makeASTFunction("arrayReduce", make_intrusive<ASTLiteral>("varPop"), std::move(samples));
            case SimpleOverTimeFunction::Present:
                return timeSeriesScalarToAST(1, context.scalar_data_type);
            case SimpleOverTimeFunction::None:
                break;
        }

        UNREACHABLE();
    }

    ASTPtr buildSimpleOverTimeValues(
        SimpleOverTimeFunction function,
        ASTPtr timestamps,
        ASTPtr values,
        TimestampType start_time,
        TimestampType end_time,
        DurationType step,
        DurationType window,
        const ConverterContext & context)
    {
        auto grid_timestamp = make_intrusive<ASTIdentifier>("t");
        auto window_ast = timeSeriesDurationToAST(window, context.timestamp_data_type);
        auto samples = buildWindowSamples(grid_timestamp->clone(), timestamps->clone(), values->clone(), window_ast->clone());
        auto value = makeASTFunction(
            "if",
            makeASTFunction("empty", samples->clone()),
            make_intrusive<ASTLiteral>(Field{}),
            buildSimpleOverTimeValue(function, std::move(samples), context));

        return makeASTFunction(
            "arrayMap",
            makeASTLambda({"t"}, std::move(value)),
            makeASTFunction(
                "timeSeriesRange",
                timeSeriesTimestampToAST(start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(step, context.timestamp_data_type)));
    }

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

            {"avg_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Avg,
             }},

            {"min_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Min,
             }},

            {"max_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Max,
             }},

            {"sum_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Sum,
             }},

            {"count_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Count,
             }},

            {"stddev_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Stddev,
             }},

            {"stdvar_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Stdvar,
             }},

            {"present_over_time",
             {
                 "",
                 /* drop_metric_name = */ true,
                 SimpleOverTimeFunction::Present,
             }},

            /// TODO:
            /// resets
            /// predict_linear
            /// deriv
            /// quantile_over_time
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
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
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

    SQLQueryPiece res = argument;
    res.node = node;
    res.type = ResultType::INSTANT_VECTOR;

    bool has_group = false;
    bool group_by_group = false;
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
            group_by_group = (impl_info->simple_over_time_function == SimpleOverTimeFunction::None);

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
            group_by_group = true;

            if (impl_info->simple_over_time_function != SimpleOverTimeFunction::None)
            {
                timestamps = makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
                values = makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Value));
            }
            else
            {
                timestamps = make_intrusive<ASTIdentifier>(ColumnNames::Timestamp);
                values = make_intrusive<ASTIdentifier>(ColumnNames::Value);
            }
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

    if (impl_info->simple_over_time_function != SimpleOverTimeFunction::None)
    {
        builder.select_list.push_back(buildSimpleOverTimeValues(
            impl_info->simple_over_time_function,
            std::move(timestamps),
            std::move(values),
            start_time,
            end_time,
            step,
            window,
            context));
    }
    else
    {
        /// <aggregate_function>(<timestamps>, <values>) AS values
        builder.select_list.push_back(addParametersToAggregateFunction(
            makeASTFunction(impl_info->ch_function_name, std::move(timestamps), std::move(values)),
            timeSeriesTimestampToAST(start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type)));
    }

    builder.select_list.back()->setAlias(ColumnNames::Values);

    if (group_by_group)
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
