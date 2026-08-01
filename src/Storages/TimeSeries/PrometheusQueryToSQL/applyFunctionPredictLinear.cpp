#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionPredictLinear.h>

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
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{

/// The name of the ClickHouse aggregate function that implements `predict_linear` on a time grid.
/// It is the `is_predict = true` variant of the linear-regression aggregate
/// (see AggregateFunctionTimeseriesLinearRegression.h) and accepts a fifth parameter,
/// `predict_offset`, in seconds.
constexpr std::string_view ch_function_name = "timeSeriesPredictLinearToGrid";

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
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto function_name = function_node->function_name;
    checkArgumentTypes(function_name, arguments, context);

    /// The second argument is the prediction horizon `t` in seconds. It is passed to the
    /// aggregate function as the `predict_offset` parameter. The aggregate registration
    /// (see AggregateFunctionTimeseriesHelpers.cpp) multiplies this value by the timestamp
    /// scale multiplier internally, so we pass it as a plain Float64 in seconds.
    auto & scalar_argument = arguments[1];
    if (scalar_argument.store_method != StoreMethod::CONST_SCALAR)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Function '{}' currently requires a constant scalar as the second argument (the prediction horizon), "
                        "but expression {} is not a constant scalar",
                        function_name, getPromQLText(scalar_argument, context));
    }

    const Float64 predict_offset_seconds = scalar_argument.scalar_value;

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto argument = std::move(arguments[0]);

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

    /// timeSeriesPredictLinearToGrid(start_timestamp, end_timestamp, grid_step, staleness, predict_offset)(timestamps, values)
    ///
    /// The first four parameters (start, end, step, window) are the same as for the other range
    /// functions. The fifth parameter, `predict_offset`, is the prediction horizon `t` in seconds,
    /// passed as a Float64 literal. The aggregate registration multiplies it by the timestamp scale
    /// multiplier to convert it to the internal timestamp units.
    builder.select_list.push_back(addParametersToAggregateFunction(
        makeASTFunction(std::string{ch_function_name}, std::move(timestamps), std::move(values)),
        timeSeriesTimestampToAST(start_time, context.timestamp_data_type),
        timeSeriesTimestampToAST(end_time, context.timestamp_data_type),
        timeSeriesDurationToAST(step, context.timestamp_data_type),
        timeSeriesDurationToAST(window, context.timestamp_data_type),
        make_intrusive<ASTLiteral>(predict_offset_seconds)));

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
