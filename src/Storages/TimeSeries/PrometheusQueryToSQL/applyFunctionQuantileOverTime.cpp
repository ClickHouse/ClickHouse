#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionQuantileOverTime.h>

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

#include <limits>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{

/// The name of the ClickHouse aggregate function that implements `quantile_over_time` on a time grid.
///
/// It follows the same convention as the other `timeSeries*ToGrid` aggregates
/// (see AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.cpp):
///   timeSeriesQuantileToGrid(start_timestamp, end_timestamp, grid_step, staleness_window, phi)(timestamp, value)
/// It buckets the samples into per-grid-point windows and returns, for each grid point, the
/// phi-quantile of the values inside that window (NULL if the window is empty).
///
/// NOTE: This aggregate function is not part of the upstream ClickHouse `timeSeries*ToGrid` family yet.
/// It must be registered in AggregateFunctions/TimeSeries (see the "Required shared changes" note in the
/// task report). The translator layer is written against that contract.
constexpr std::string_view ch_function_name = "timeSeriesQuantileToGrid";

/// `quantile_over_time` always drops the metric name (PromQL: function outputs have no `__name__`).
constexpr bool drop_metric_name = true;


/// Checks that the arguments are valid for `quantile_over_time`:
///   - exactly 2 arguments
///   - first argument is a SCALAR (the quantile parameter `phi`)
///   - second argument is a RANGE_VECTOR
void checkArgumentTypes(std::string_view function_name, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
{
    if (arguments.size() != 2)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects 2 arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

    const auto & phi_argument = arguments[0];
    if (phi_argument.type != ResultType::SCALAR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function {} expects first argument of type {}, but expression {} has type {}",
                        function_name, ResultType::SCALAR,
                        getPromQLText(phi_argument, context), phi_argument.type);
    }

    const auto & range_argument = arguments[1];
    if (range_argument.type != ResultType::RANGE_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function {} expects second argument of type {}, but expression {} has type {}",
                        function_name, ResultType::RANGE_VECTOR,
                        getPromQLText(range_argument, context), range_argument.type);
    }
}


/// Builds a fresh AST expression for the scalar parameter `phi`.
///
/// `phi` may be either a compile-time constant (`CONST_SCALAR`, produced by a float literal) or a
/// single-row scalar subquery (`SINGLE_SCALAR`). A non-constant scalar grid (`SCALAR_GRID`) is not
/// supported, mirroring the `quantile` aggregation operator (see applyAggregationOperatorQuantile.cpp).
///
/// The returned AST is built fresh on every call so that it can be inserted into several places of
/// the generated SQL tree (the aggregate-function parameter and the edge-case guards) without sharing
/// AST nodes between parents.
struct PhiSource
{
    StoreMethod store_method = StoreMethod::EMPTY;
    Float64 const_value = 0.0;
    ASTPtr select_query;
    bool subquery_registered = false;
    String subquery_name;
};

ASTPtr makePhiAST(PhiSource & phi_source, ConverterContext & context)
{
    switch (phi_source.store_method)
    {
        case StoreMethod::CONST_SCALAR:
            return timeSeriesScalarToAST(phi_source.const_value, context.scalar_data_type);

        case StoreMethod::SINGLE_SCALAR:
        {
            if (!phi_source.subquery_registered)
            {
                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(phi_source.select_query), SQLSubqueryType::SCALAR});
                phi_source.subquery_name = context.subqueries.back().name;
                phi_source.subquery_registered = true;
            }
            /// Wrap with assumeNotNull() because scalar subqueries make their result nullable,
            /// but StoreMethod::SINGLE_SCALAR always means one row.
            return makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(phi_source.subquery_name));
        }

        case StoreMethod::SCALAR_GRID:
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Function 'quantile_over_time' with a non-constant scalar parameter `phi` is not supported");
        }

        default:
        {
            /// EMPTY / CONST_STRING / VECTOR_GRID / RAW_DATA are rejected by checkArgumentTypes()
            /// (wrong ResultType) and never reach here.
            throwUnexpectedStoreMethod({nullptr, ResultType::SCALAR, phi_source.store_method}, context);
        }
    }
}


/// Wraps the quantile-grid array produced by `timeSeriesQuantileToGrid` so that the Prometheus
/// edge cases for the `phi` parameter are honored at every grid point:
///   - phi < 0   -> -Inf
///   - phi > 1   -> +Inf
///   - phi = NaN -> NaN
/// Grid points whose window had no samples are returned as NULL by the aggregate and are left
/// untouched (the series is absent at that point), which matches Prometheus behaviour.
///
/// Generates: arrayMap(x -> if(isNull(x), x,
///                            if(isNaN(phi), NaN,
///                               if(less(phi, 0), -Inf,
///                                  if(greater(phi, 1), +Inf, x)))), <quantile_grid>)
ASTPtr wrapWithPhiEdgeCases(ASTPtr && quantile_grid, PhiSource & phi_source, ConverterContext & context)
{
    auto make_x = [] { return make_intrusive<ASTIdentifier>("x"); };

    auto nan_ast = timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN(), context.scalar_data_type);
    auto neg_inf_ast = timeSeriesScalarToAST(-std::numeric_limits<Float64>::infinity(), context.scalar_data_type);
    auto pos_inf_ast = timeSeriesScalarToAST(std::numeric_limits<Float64>::infinity(), context.scalar_data_type);

    ASTPtr body = makeASTFunction(
        "if",
        makeASTFunction("isNull", make_x()),
        make_x(), /// NULL passthrough: keeps the nullable type and the "absent series" semantics
        makeASTFunction(
            "if",
            makeASTFunction("isNaN", makePhiAST(phi_source, context)),
            std::move(nan_ast),
            makeASTFunction(
                "if",
                makeASTFunction("less", makePhiAST(phi_source, context), make_intrusive<ASTLiteral>(0.0)),
                std::move(neg_inf_ast),
                makeASTFunction(
                    "if",
                    makeASTFunction("greater", makePhiAST(phi_source, context), make_intrusive<ASTLiteral>(1.0)),
                    std::move(pos_inf_ast),
                    make_x()))));

    return makeASTFunction(
        "arrayMap",
        makeASTFunction("lambda", makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")), std::move(body)),
        std::move(quantile_grid));
}

}


bool isFunctionQuantileOverTime(std::string_view function_name)
{
    return function_name == "quantile_over_time";
}


SQLQueryPiece applyFunctionQuantileOverTime(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto function_name = function_node->function_name;
    checkArgumentTypes(function_name, arguments, context);

    PhiSource phi_source;
    {
        auto & phi_argument = arguments[0];
        phi_source.store_method = phi_argument.store_method;
        phi_source.const_value = phi_argument.scalar_value;
        phi_source.select_query = phi_argument.select_query;
    }

    auto & range_argument = arguments[1];

    /// If either argument is empty then the result is also empty.
    if (phi_source.store_method == StoreMethod::EMPTY || range_argument.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto argument = std::move(range_argument);

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
            /// SELECT arrayMap(...,
            ///             timeSeriesQuantileToGrid(<start>, <end>, <step>, <window>, phi)(
            ///                 timeSeriesRange(<start_time>, <end_time>, <step>),
            ///                 arrayResize([], <count_of_time_steps>, <scalar_value>))) AS values
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
            /// SELECT arrayMap(...,
            ///             timeSeriesQuantileToGrid(<start>, <end>, <step>, <window>, phi)(
            ///                 timeSeriesRange(<start_time>, <end_time>, <step>), values)) AS values
            /// FROM <scalar_grid>
            values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT group,
            ///        arrayMap(...,
            ///            timeSeriesQuantileToGrid(<start>, <end>, <step>, <window>, phi)(
            ///                (timeSeriesFromGrid(<start_time>, <end_time>, <step>, values) AS time_series).1,
            ///                time_series.2)) AS values
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
            ///        arrayMap(...,
            ///            timeSeriesQuantileToGrid(<start>, <end>, <step>, <window>, phi)(timestamp, value)) AS values
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

    /// timeSeriesQuantileToGrid(start_timestamp, end_timestamp, grid_step, staleness_window, phi)(timestamps, values)
    ///
    /// The first four parameters (start, end, step, window) are the same as for the other range
    /// functions. The fifth parameter, `phi`, is the quantile level in [0, 1] (out-of-range / NaN
    /// values are handled by the wrapping arrayMap below).
    ASTPtr quantile_grid = addParametersToAggregateFunction(
        makeASTFunction(std::string{ch_function_name}, std::move(timestamps), std::move(values)),
        timeSeriesTimestampToAST(start_time, context.timestamp_data_type),
        timeSeriesTimestampToAST(end_time, context.timestamp_data_type),
        timeSeriesDurationToAST(step, context.timestamp_data_type),
        timeSeriesDurationToAST(window, context.timestamp_data_type),
        makePhiAST(phi_source, context));

    /// Apply the phi edge-case (phi < 0 -> -Inf, phi > 1 -> +Inf, phi = NaN -> NaN) per grid point,
    /// keeping NULLs for empty windows.
    builder.select_list.push_back(wrapWithPhiEdgeCases(std::move(quantile_grid), phi_source, context));
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
