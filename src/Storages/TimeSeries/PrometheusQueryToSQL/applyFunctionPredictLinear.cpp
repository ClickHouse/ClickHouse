#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionPredictLinear.h>

#include <Core/DecimalFunctions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionTime.h>
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

/// Constant/single-row prediction horizon: `predict_offset` is a fixed aggregate-function parameter.
constexpr std::string_view ch_function_name = "timeSeriesPredictLinearToGrid";
/// Per-grid-point-varying horizon (e.g. `predict_linear(v[5m], time())`): `predict_offset` is a
/// third Array(Float64) argument instead. See AggregateFunctionTimeseriesPredictLinearVarying.h.
constexpr std::string_view ch_function_name_varying = "timeSeriesPredictLinearVaryingToGrid";

/// `predict_linear` always drops the metric name (PromQL: function outputs have no `__name__`).
constexpr bool drop_metric_name = true;


/// Checks that the arguments are valid for `predict_linear`:
///   - exactly 2 arguments
///   - first argument is a RANGE_VECTOR
///   - second argument is a SCALAR
void checkArgumentTypes(std::string_view function_name, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
{
    if (arguments.size() != 2)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects 2 arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

    const auto & range_argument = arguments[0];
    if (range_argument.type != ResultType::RANGE_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function {} expects first argument of type {}, but expression {} has type {}",
                        function_name, ResultType::RANGE_VECTOR,
                        getPromQLText(range_argument, context), range_argument.type);
    }

    const auto & scalar_argument = arguments[1];
    if (scalar_argument.type != ResultType::SCALAR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function {} expects second argument of type {}, but expression {} has type {}",
                        function_name, ResultType::SCALAR,
                        getPromQLText(scalar_argument, context), scalar_argument.type);
    }
}

}


bool isFunctionPredictLinear(std::string_view function_name)
{
    return function_name == "predict_linear";
}


SQLQueryPiece applyFunctionPredictLinear(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto function_name = function_node->function_name;
    checkArgumentTypes(function_name, arguments, context);

    /// A fixed @ on the range vector makes the whole call step-invariant in PromQL, so it is evaluated once.
    const auto * fixed_at_node = getFixedAtModifier(arguments[0]);

    /// The second argument (`t`, prediction horizon in seconds) may be a constant, a single-row
    /// scalar subquery, or (e.g. `time()` in a range query) a scalar grid -- one value per grid point.
    auto & scalar_argument = arguments[1];
    scalar_argument = makeVaryingScalarPrecisionSafe(
        function_name, function_node->getArguments()[1], std::move(scalar_argument), context);
    ASTPtr predict_offset_ast;
    bool varying_predict_offset = false;
    switch (scalar_argument.store_method)
    {
        case StoreMethod::CONST_SCALAR:
            predict_offset_ast = make_intrusive<ASTLiteral>(scalar_argument.scalar_value);
            break;

        case StoreMethod::SINGLE_SCALAR:
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
            /// assumeNotNull(): scalar subqueries make their result nullable, but SINGLE_SCALAR is always one row.
            predict_offset_ast = makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(context.subqueries.back().name));
            break;

        case StoreMethod::SCALAR_GRID:
            if (fixed_at_node)
            {
                /// A fixed @ freezes the samples but not the horizon, so PromQL still evaluates per step; the
                /// varying aggregate derives its window from each grid point and cannot express a frozen window.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Function '{}' does not support a time-varying second argument (the prediction horizon) "
                                "together with a fixed @ modifier on the range vector {}",
                                function_name, getPromQLText(arguments[0], context));
            }
            varying_predict_offset = true;
            /// A scalar grid is always exactly one row with one Array column (see e.g. fromFunctionTime.cpp),
            /// so it can be registered and referenced the same way as SINGLE_SCALAR above, no JOIN needed.
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
            /// The grid is Array of either the scalar or (for a time() grid kept at full precision) the
            /// timestamp type; normalized here so the aggregate sees one type in every case.
            predict_offset_ast = makeASTFunction(
                "CAST", make_intrusive<ASTIdentifier>(context.subqueries.back().name), make_intrusive<ASTLiteral>("Array(Float64)"));
            break;

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Function '{}' currently requires a constant, a single-row scalar, or a scalar grid as the "
                            "second argument (the prediction horizon), but expression {} is not supported",
                            function_name, getPromQLText(scalar_argument, context));
    }

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto argument = std::move(arguments[0]);

    const auto aggregation_range = getRangeAggregationRange(fixed_at_node, node_range, context);

    if (fixed_at_node)
    {
        /// A fixed @ freezes only the sample window; PromQL still evaluates a step-invariant predict_linear at the
        /// range start. The fit is linear, so predicting that much further ahead moves the origin there exactly.
        const Float64 evaluation_time_shift = DecimalUtils::convertTo<Float64>(
            DurationType{start_time.value - aggregation_range.start_time.value}, context.timestamp_scale);

        if (evaluation_time_shift != 0)
            predict_offset_ast = makeASTFunction(
                "plus", std::move(predict_offset_ast), make_intrusive<ASTLiteral>(evaluation_time_shift));
    }

    SQLQueryPiece res = argument;
    res.node = function_node;
    res.type = ResultType::INSTANT_VECTOR;

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
            ///                             values) AS values
            /// FROM <scalar_grid>
            values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT group,
            ///        <aggregate_function>((timeSeriesFromGrid(<start_time>, <end_time>, <step>, values) AS time_series).1,
            ///                             time_series.2) AS values
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

    /// Constant/single-row horizon: predict_offset is the aggregate's 5th parameter.
    /// Varying horizon: predict_offset is a 3rd argument instead, one value per grid point.
    ASTPtr aggregate_values;
    if (varying_predict_offset)
    {
        aggregate_values = addParametersToAggregateFunction(
            makeASTFunction(std::string{ch_function_name_varying}, std::move(timestamps), std::move(values), std::move(predict_offset_ast)),
            timeSeriesTimestampToAST(aggregation_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(aggregation_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(aggregation_range.step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type));
    }
    else
    {
        aggregate_values = addParametersToAggregateFunction(
            makeASTFunction(std::string{ch_function_name}, std::move(timestamps), std::move(values)),
            timeSeriesTimestampToAST(aggregation_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(aggregation_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(aggregation_range.step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type),
            std::move(predict_offset_ast));
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

    res.select_query = builder.getSelectQuery();
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;

    if (has_group && drop_metric_name)
        res = dropMetricName(std::move(res), context);

    return res;
}

}
