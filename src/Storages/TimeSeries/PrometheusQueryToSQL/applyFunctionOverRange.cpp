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

    ASTPtr toFloat64(ASTPtr && x)
    {
        return makeASTFunction("toFloat64", std::move(x));
    }

    void checkPredictLinearArgumentTypes(const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        std::string_view function_name = "predict_linear";
        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 2 arguments, but was called with {} arguments",
                            function_name, arguments.size());
        }

        const auto & range_arg = arguments[0];
        if (range_arg.type != ResultType::RANGE_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::RANGE_VECTOR,
                            getPromQLText(range_arg, context), range_arg.type);
        }

        const auto & scalar_arg = arguments[1];
        if (scalar_arg.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects second argument of type {}, but expression {} has type {}",
                            function_name, ResultType::SCALAR,
                            getPromQLText(scalar_arg, context), scalar_arg.type);
        }
    }

    ASTPtr getStaticScalarParameter(SQLQueryPiece && scalar_arg, ConverterContext & context)
    {
        switch (scalar_arg.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                return timeSeriesScalarToAST(scalar_arg.scalar_value, context.scalar_data_type);
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_arg.select_query), SQLSubqueryType::SCALAR});
                auto subquery_id = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                /// Wrap with `assumeNotNull` because scalar subqueries make their result nullable,
                /// but StoreMethod::SINGLE_SCALAR always means one row.
                return makeASTFunction("assumeNotNull", std::move(subquery_id));
            }
            default:
            {
                throwUnexpectedStoreMethod(scalar_arg, context);
            }
        }
    }

    ASTPtr makeTimeGrid(TimestampType start_time, TimestampType end_time, DurationType step, const ConverterContext & context)
    {
        return makeASTFunction(
            "timeSeriesRange",
            timeSeriesTimestampToAST(start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(step, context.timestamp_data_type));
    }

    struct PredictLinearTimestampShift
    {
        ASTPtr constant_value;
        ASTPtr grid_values;
    };

    /// `predict_linear` predicts the value at `evaluation_timestamp + predict_offset`, so an `offset` or `@` modifier
    /// applied to the range-vector argument shifts the prediction target relative to the aggregation timestamp.
    PredictLinearTimestampShift getPredictLinearTimestampShift(
        const SQLQueryPiece & range_argument,
        TimestampType start_time,
        TimestampType end_time,
        DurationType step,
        const ConverterContext & context)
    {
        if (!range_argument.node || range_argument.node->node_type != NodeType::Offset)
            return {};

        const auto * offset_node = static_cast<const PrometheusQueryTree::Offset *>(range_argument.node);
        if (offset_node->hasAtModifier())
        {
            const auto & expression_range = context.node_range_getter.get(offset_node->getExpression());
            auto fixed_evaluation_timestamp = timeSeriesTimestampToAST(expression_range.start_time, context.timestamp_data_type);

            auto grid_values = makeASTFunction(
                "arrayMap",
                makeASTLambda(
                    {"evaluation_timestamp"},
                    toFloat64(makeASTFunction(
                        "minus",
                        make_intrusive<ASTIdentifier>("evaluation_timestamp"),
                        std::move(fixed_evaluation_timestamp)))),
                makeTimeGrid(start_time, end_time, step, context));

            return PredictLinearTimestampShift{nullptr, std::move(grid_values)};
        }

        if (offset_node->offset_value)
        {
            auto constant_value = toFloat64(timeSeriesDurationToAST(*offset_node->offset_value, context.timestamp_data_type));
            return PredictLinearTimestampShift{std::move(constant_value), nullptr};
        }

        return {};
    }

    ASTPtr makePredictOffsetArray(ASTPtr && predict_offset, TimestampType start_time, TimestampType end_time, DurationType step)
    {
        return makeASTFunction(
            "arrayResize",
            make_intrusive<ASTLiteral>(Array{}),
            make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(start_time, end_time, step)),
            std::move(predict_offset));
    }

    ASTPtr addConstantPredictOffsetShift(ASTPtr && predict_offsets, ASTPtr && timestamp_shift)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTLambda(
                {"predict_offset"},
                makeASTFunction(
                    "plus",
                    make_intrusive<ASTIdentifier>("predict_offset"),
                    std::move(timestamp_shift))),
            std::move(predict_offsets));
    }

    ASTPtr addGridPredictOffsetShift(ASTPtr && predict_offsets, ASTPtr && timestamp_shifts)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTLambda(
                {"predict_offset", "timestamp_shift"},
                makeASTFunction(
                    "plus",
                    make_intrusive<ASTIdentifier>("predict_offset"),
                    make_intrusive<ASTIdentifier>("timestamp_shift"))),
            std::move(predict_offsets),
            std::move(timestamp_shifts));
    }

    ASTPtr applyPredictLinearScalarGridParameter(
        ASTPtr && intercept_values,
        ASTPtr && slope_values,
        ASTPtr && predict_offsets)
    {
        auto is_null = makeASTFunction(
            "or",
            makeASTFunction("isNull", make_intrusive<ASTIdentifier>("intercept")),
            makeASTFunction("isNull", make_intrusive<ASTIdentifier>("slope")));

        /// `timeSeriesDerivToGrid` reports a per-second slope and `predict_offset` is expressed in seconds,
        /// so the prediction needs no conversion to the timestamp tick resolution here.
        auto prediction = makeASTFunction(
            "plus",
            makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>("intercept")),
            makeASTFunction(
                "multiply",
                makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>("slope")),
                make_intrusive<ASTIdentifier>("predict_offset")));

        return makeASTFunction(
            "arrayMap",
            makeASTLambda(
                {"intercept", "slope", "predict_offset"},
                makeASTFunction(
                    "if",
                    std::move(is_null),
                    make_intrusive<ASTLiteral>(Field{}),
                    std::move(prediction))),
            std::move(intercept_values),
            std::move(slope_values),
            std::move(predict_offsets));
    }

    /// A fixed @ expression is evaluated once by Prometheus. Repeat the single aggregate result on the outer
    /// query grid instead of sliding the range function over the outer evaluation timestamps.
    ASTPtr repeatFixedAggregateOnGrid(ASTPtr && aggregate_values, size_t result_grid_size, size_t aggregation_grid_size)
    {
        return makeASTFunction(
            "arrayResize",
            make_intrusive<ASTLiteral>(Array{}),
            make_intrusive<ASTLiteral>(result_grid_size),
            makeASTFunction("arrayElement", std::move(aggregate_values), make_intrusive<ASTLiteral>(aggregation_grid_size)));
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

            /// TODO:
            /// avg_over_time
            /// min_over_time
            /// max_over_time
            /// sum_over_time
            /// count_over_time
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
    return (function_name == "predict_linear") || (getImplInfo(function_name) != nullptr);
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
    const bool is_predict_linear = (function_name == "predict_linear");
    chassert(impl_info || is_predict_linear);

    std::string_view ch_function_name;
    bool drop_metric_name = true;
    ASTPtr extra_parameter;
    ASTPtr grid_parameter;

    if (is_predict_linear)
    {
        checkPredictLinearArgumentTypes(arguments, context);
        ch_function_name = "timeSeriesPredictLinearToGrid";
    }
    else
    {
        checkArgumentTypes(function_name, arguments, context);
        ch_function_name = impl_info->ch_function_name;
        drop_metric_name = impl_info->drop_metric_name;
    }

    auto node_range = context.node_range_getter.get(node);
    if (node_range.empty())
        return SQLQueryPiece{node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    if (is_predict_linear)
    {
        if (arguments[0].store_method == StoreMethod::EMPTY || arguments[1].store_method == StoreMethod::EMPTY)
            return SQLQueryPiece{node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

        if (arguments[1].store_method == StoreMethod::SCALAR_GRID)
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(arguments[1].select_query), SQLSubqueryType::SCALAR});
            grid_parameter = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
        }
        else
        {
            extra_parameter = getStaticScalarParameter(std::move(arguments[1]), context);
        }
    }

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto argument = std::move(arguments[0]);

    if (is_predict_linear)
    {
        auto timestamp_shift = getPredictLinearTimestampShift(argument, start_time, end_time, step, context);
        if (grid_parameter)
        {
            if (timestamp_shift.constant_value)
                grid_parameter = addConstantPredictOffsetShift(std::move(grid_parameter), std::move(timestamp_shift.constant_value));
            else if (timestamp_shift.grid_values)
                grid_parameter = addGridPredictOffsetShift(std::move(grid_parameter), std::move(timestamp_shift.grid_values));
        }
        else if (extra_parameter)
        {
            if (timestamp_shift.constant_value)
            {
                extra_parameter = makeASTFunction("plus", std::move(extra_parameter), std::move(timestamp_shift.constant_value));
            }
            else if (timestamp_shift.grid_values)
            {
                grid_parameter = addGridPredictOffsetShift(
                    makePredictOffsetArray(std::move(extra_parameter), start_time, end_time, step),
                    std::move(timestamp_shift.grid_values));
                extra_parameter = nullptr;
            }
        }
    }

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

            if (is_predict_linear)
            {
                /// `predict_linear` builds two aggregate functions over the same time series, and the alias
                /// must not be defined twice, so the expression is repeated instead of aliased.
                timestamps = makeASTFunction("tupleElement", ts->clone(), make_intrusive<ASTLiteral>(1));
                values = makeASTFunction("tupleElement", std::move(ts), make_intrusive<ASTLiteral>(2));
            }
            else
            {
                ts->setAlias(ColumnNames::TimeSeries);
                timestamps = makeASTFunction("tupleElement", std::move(ts), make_intrusive<ASTLiteral>(1));

                /// time_series.2
                values = makeASTFunction(
                    "tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries), make_intrusive<ASTLiteral>(2));
            }

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
    ASTPtr aggregate_values;
    if (grid_parameter)
    {
        /// The aggregate function parameter must be constant for `timeSeriesPredictLinearToGrid`,
        /// but PromQL allows the `predict_linear` offset to be any scalar expression. Compute
        /// `predict_linear(v, offset)` as the prediction at the evaluation timestamp plus the
        /// regression slope multiplied by the per-step offset.
        ASTPtr intercept_values = addParametersToAggregateFunction(
            makeASTFunction(ch_function_name, timestamps->clone(), values->clone()),
            timeSeriesTimestampToAST(aggregation_start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(aggregation_end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(aggregation_step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type),
            timeSeriesScalarToAST(0, context.scalar_data_type));

        ASTPtr slope_values = addParametersToAggregateFunction(
            makeASTFunction("timeSeriesDerivToGrid", std::move(timestamps), std::move(values)),
            timeSeriesTimestampToAST(aggregation_start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(aggregation_end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(aggregation_step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type));

        if (fixed_at_node)
        {
            intercept_values = repeatFixedAggregateOnGrid(std::move(intercept_values), result_grid_size, aggregation_grid_size);
            slope_values = repeatFixedAggregateOnGrid(std::move(slope_values), result_grid_size, aggregation_grid_size);
        }

        aggregate_values = applyPredictLinearScalarGridParameter(
            std::move(intercept_values), std::move(slope_values), std::move(grid_parameter));
    }
    else
    {
        auto aggregate_function = makeASTFunction(ch_function_name, std::move(timestamps), std::move(values));
        if (extra_parameter)
        {
            aggregate_values = addParametersToAggregateFunction(
                std::move(aggregate_function),
                timeSeriesTimestampToAST(aggregation_start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(aggregation_end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(aggregation_step, context.timestamp_data_type),
                timeSeriesDurationToAST(window, context.timestamp_data_type),
                std::move(extra_parameter));
        }
        else
        {
            aggregate_values = addParametersToAggregateFunction(
                std::move(aggregate_function),
                timeSeriesTimestampToAST(aggregation_start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(aggregation_end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(aggregation_step, context.timestamp_data_type),
                timeSeriesDurationToAST(window, context.timestamp_data_type));
        }

        if (fixed_at_node)
            aggregate_values = repeatFixedAggregateOnGrid(std::move(aggregate_values), result_grid_size, aggregation_grid_size);
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

    if (has_group && drop_metric_name)
        res = dropMetricName(std::move(res), context);

    return res;
}

}
