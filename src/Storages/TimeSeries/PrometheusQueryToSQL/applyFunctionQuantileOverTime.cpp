#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionQuantileOverTime.h>

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
/// Per-grid-point-varying phi (e.g. `quantile_over_time(scalar(...), v[5m])` with a time-varying
/// scalar): phi is a third Array(Float64) argument. See AggregateFunctionTimeseriesQuantileVarying.h.
constexpr std::string_view ch_function_name_varying = "timeSeriesQuantileVaryingToGrid";

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


/// Builds a fresh AST for the constant/single-row `phi` (CONST_SCALAR/SINGLE_SCALAR); built fresh each
/// call since AST nodes can't be shared between parents. Varying (SCALAR_GRID) phi bypasses this function.
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
            /// Keep the literal at Float64: casting phi to a Float32 scalar type would round
            /// e.g. 1.00000003 to 1.0 and hide the phi > 1 edge case below.
            return make_intrusive<ASTLiteral>(phi_source.const_value);

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

        default:
        {
            /// EMPTY / CONST_STRING / VECTOR_GRID / RAW_DATA are rejected by checkArgumentTypes() (wrong
            /// ResultType); SCALAR_GRID is routed to the varying-phi path by the caller before reaching here.
            throwUnexpectedStoreMethod({nullptr, ResultType::SCALAR, phi_source.store_method}, context);
        }
    }
}


/// Builds the same if(isNull, ..., if(isNaN(phi), ..., ...)) edge-case body, for a `phi` AST that's
/// either a constant expression (shared across all grid points) or the "phi" lambda variable (one per point).
ASTPtr makePhiEdgeCaseBody(const ASTPtr & phi_ast, ASTPtr && nan_ast, ASTPtr && neg_inf_ast, ASTPtr && pos_inf_ast)
{
    auto make_x = [] { return make_intrusive<ASTIdentifier>("x"); };
    return makeASTFunction(
        "if",
        makeASTFunction("isNull", make_x()),
        make_x(), /// NULL passthrough: keeps the nullable type and the "absent series" semantics
        makeASTFunction(
            "if",
            makeASTFunction("isNaN", phi_ast->clone()),
            std::move(nan_ast),
            makeASTFunction(
                "if",
                makeASTFunction("less", phi_ast->clone(), make_intrusive<ASTLiteral>(0.0)),
                std::move(neg_inf_ast),
                makeASTFunction(
                    "if",
                    makeASTFunction("greater", phi_ast->clone(), make_intrusive<ASTLiteral>(1.0)),
                    std::move(pos_inf_ast),
                    make_x()))));
}

/// Wraps the quantile-grid array so the Prometheus phi edge cases (phi<0 -> -Inf, phi>1 -> +Inf,
/// phi=NaN -> NaN) are honored per grid point; NULLs (empty window) pass through untouched.
ASTPtr wrapWithPhiEdgeCases(ASTPtr && quantile_grid, PhiSource & phi_source, ConverterContext & context)
{
    auto nan_ast = timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN(), context.scalar_data_type);
    auto neg_inf_ast = timeSeriesScalarToAST(-std::numeric_limits<Float64>::infinity(), context.scalar_data_type);
    auto pos_inf_ast = timeSeriesScalarToAST(std::numeric_limits<Float64>::infinity(), context.scalar_data_type);

    ASTPtr body = makePhiEdgeCaseBody(
        makePhiAST(phi_source, context), std::move(nan_ast), std::move(neg_inf_ast), std::move(pos_inf_ast));

    return makeASTFunction(
        "arrayMap",
        makeASTFunction("lambda", makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")), std::move(body)),
        std::move(quantile_grid));
}

/// Per-grid-point-varying phi sibling of wrapWithPhiEdgeCases: phi comes from `phi_array_ast` (one value
/// per grid point) instead of a single constant, so the edge cases are checked per point too.
ASTPtr wrapWithVaryingPhiEdgeCases(ASTPtr && quantile_grid, ASTPtr && phi_array_ast, ConverterContext & context)
{
    auto nan_ast = timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN(), context.scalar_data_type);
    auto neg_inf_ast = timeSeriesScalarToAST(-std::numeric_limits<Float64>::infinity(), context.scalar_data_type);
    auto pos_inf_ast = timeSeriesScalarToAST(std::numeric_limits<Float64>::infinity(), context.scalar_data_type);

    ASTPtr body = makePhiEdgeCaseBody(
        make_intrusive<ASTIdentifier>("phi"), std::move(nan_ast), std::move(neg_inf_ast), std::move(pos_inf_ast));

    return makeASTFunction(
        "arrayMap",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("phi")),
            std::move(body)),
        std::move(quantile_grid),
        std::move(phi_array_ast));
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

    /// A fixed @ on the range vector makes the whole call step-invariant in PromQL, so it is evaluated once.
    const auto * fixed_at_node = getFixedAtModifier(arguments[1]);

    arguments[0] = makeVaryingScalarPrecisionSafe(
        function_name, function_node->getArguments()[0], std::move(arguments[0]), context);

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

    /// A scalar grid is one row with one Array column (see fromFunctionTime.cpp), registered as a scalar
    /// subquery like SINGLE_SCALAR (no JOIN needed) -- past the empty check so it can't orphan a registration.
    const bool varying_phi = phi_source.store_method == StoreMethod::SCALAR_GRID;
    String varying_phi_subquery_name;
    if (varying_phi)
    {
        if (fixed_at_node)
        {
            /// A fixed @ freezes the samples but not phi, so PromQL still evaluates per step; the varying
            /// aggregate derives its window from each grid point and cannot express a frozen window.
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Function '{}' does not support a time-varying first argument (the quantile) together with "
                            "a fixed @ modifier on the range vector {}",
                            function_name, getPromQLText(range_argument, context));
        }
        context.subqueries.emplace_back(context.subqueries.size(), std::move(phi_source.select_query), SQLSubqueryType::SCALAR);
        varying_phi_subquery_name = context.subqueries.back().name;
    }

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto argument = std::move(range_argument);

    const auto aggregation_range = getRangeAggregationRange(fixed_at_node, node_range, context);

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

    /// Constant/single-row phi: the aggregate's 5th parameter. Varying phi: a 3rd argument instead,
    /// one value per grid point; the edge-case wrapping below also becomes per-point.
    ASTPtr quantile_grid;
    ASTPtr result_values;
    if (varying_phi)
    {
        /// The grid is Array of either the scalar or (for a time() grid kept at full precision) the timestamp
        /// type; normalized here so the aggregate and the edge-case wrapping below see one type in every case.
        auto castVaryingPhi = [&]
        {
            return makeASTFunction(
                "CAST", make_intrusive<ASTIdentifier>(varying_phi_subquery_name), make_intrusive<ASTLiteral>("Array(Float64)"));
        };
        quantile_grid = addParametersToAggregateFunction(
            makeASTFunction(std::string{ch_function_name_varying}, std::move(timestamps), std::move(values), castVaryingPhi()),
            timeSeriesTimestampToAST(aggregation_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(aggregation_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(aggregation_range.step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type));
        result_values = wrapWithVaryingPhiEdgeCases(std::move(quantile_grid), castVaryingPhi(), context);
    }
    else
    {
        quantile_grid = addParametersToAggregateFunction(
            makeASTFunction(std::string{ch_function_name}, std::move(timestamps), std::move(values)),
            timeSeriesTimestampToAST(aggregation_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(aggregation_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(aggregation_range.step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type),
            makePhiAST(phi_source, context));
        result_values = wrapWithPhiEdgeCases(std::move(quantile_grid), phi_source, context);
    }

    if (fixed_at_node)
        result_values = repeatFixedAtResultOverGrid(
            std::move(result_values), aggregation_range, stepsInTimeSeriesRange(start_time, end_time, step));

    builder.select_list.push_back(std::move(result_values));
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
