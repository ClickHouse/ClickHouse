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
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fixedAtModifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getToGridAggregateFunctionArguments.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <iterator>
#include <optional>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{

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


/// The prediction horizon (the 2nd argument of `predict_linear`, in seconds) converted to an AST.
struct PredictionOffset
{
    /// nullptr if the horizon is statically empty, then the result of `predict_linear` is empty too.
    ASTPtr ast;

    /// Whether the horizon is the same at every grid point. Then `ast` is a number: a literal or a reference to a single-row
    /// scalar subquery. Otherwise (e.g. `time()` in a range query) it varies with the evaluation time and `ast` is an array
    /// with one value per grid point.
    bool is_constant = true;
};

PredictionOffset getPredictionOffset(SQLQueryPiece & scalar_argument, ConverterContext & context)
{
    PredictionOffset prediction_offset;
    switch (scalar_argument.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        {
            prediction_offset.ast = make_intrusive<ASTLiteral>(scalar_argument.scalar_value);
            break;
        }

        case StoreMethod::SINGLE_SCALAR:
        {
            /// A scalar subquery is nullable, but it always has exactly one row here.
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
            prediction_offset.ast = makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(context.subqueries.back().name));
            break;
        }

        case StoreMethod::SCALAR_GRID:
        {
            /// A scalar grid is one row with one Array column, so it is a scalar subquery too.
            prediction_offset.is_constant = false;
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR);
            prediction_offset.ast = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
            break;
        }

        case StoreMethod::EMPTY:
        {
            break;
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::VECTOR_GRID:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here because these store methods are incompatible with a scalar (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(scalar_argument, context);
        }
    }
    return prediction_offset;
}


/// How a fixed @ modifier shifts the prediction horizons: the horizon of the grid point `i` becomes
/// `horizon + (shift_at_start + i * step_in_seconds)`, where `shift_at_start` is the distance in seconds from the frozen
/// timestamp to the first grid point.
struct HorizonShift
{
    Float64 shift_at_start;
    Float64 step_in_seconds;
    size_t grid_size;
};

/// Calculates the predictions `intercept + slope * horizon` from the result of timeSeriesLinearRegressionToGrid.
ASTPtr makePredictions(ASTPtr && regression, PredictionOffset && prediction_offset, const std::optional<HorizonShift> & horizon_shift)
{
    /// The result of timeSeriesLinearRegressionToGrid is the tuple `(intercept, slope)` for every grid point, so:
    /// arrayMap((r[, t][, i]) -> r.1 + r.2 * <horizon>, <regression>[, <horizons>][, range(<grid_size>)])
    /// where `t` is the horizon of the grid point if the horizons vary, and `i` is the index of the grid point if the
    /// horizons are shifted. NULLs (no fit in the window) pass through.
    Strings lambda_parameters{"r"};
    ASTs arrays{std::move(regression)};

    ASTPtr horizon;
    if (prediction_offset.is_constant)
    {
        horizon = std::move(prediction_offset.ast);
    }
    else
    {
        lambda_parameters.push_back("t");
        arrays.push_back(std::move(prediction_offset.ast));
        horizon = make_intrusive<ASTIdentifier>("t");
    }

    if (horizon_shift)
    {
        horizon = makeASTFunction(
            "plus",
            std::move(horizon),
            makeASTFunction(
                "plus",
                make_intrusive<ASTLiteral>(horizon_shift->shift_at_start),
                makeASTFunction("multiply", make_intrusive<ASTIdentifier>("i"), make_intrusive<ASTLiteral>(horizon_shift->step_in_seconds))));
        lambda_parameters.push_back("i");
        arrays.push_back(makeASTFunction("range", make_intrusive<ASTLiteral>(horizon_shift->grid_size)));
    }

    ASTPtr prediction = makeASTFunction(
        "plus",
        makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("r"), make_intrusive<ASTLiteral>(1u)),
        makeASTFunction(
            "multiply",
            makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("r"), make_intrusive<ASTLiteral>(2u)),
            std::move(horizon)));

    ASTs array_map_arguments{makeASTLambda(lambda_parameters, std::move(prediction))};
    array_map_arguments.insert(array_map_arguments.end(), std::make_move_iterator(arrays.begin()), std::make_move_iterator(arrays.end()));
    return makeASTFunction("arrayMap", std::move(array_map_arguments));
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

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto range_argument = std::move(arguments[0]);

    if (range_argument.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY}; /// The range vector is empty, so is the result.

    /// The horizon goes last: it may register a scalar subquery, which is left unused if the result is found empty above.
    PredictionOffset prediction_offset = getPredictionOffset(arguments[1], context);
    if (!prediction_offset.ast)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    ASTs aggregate_function_arguments = getToGridAggregateFunctionArguments(range_argument, context);

    /// A fixed @ on the range vector freezes the sample window at the fixed timestamp.
    const auto * fixed_at_node = getFixedAtModifier(range_argument);
    const auto aggregation_range = getRangeAggregationRange(fixed_at_node, node_range, context);
    const size_t result_grid_size = stepsInTimeSeriesRange(start_time, end_time, step);

    /// The result is a vector grid (one row per series, the aggregate function is calculated `GROUP BY group`) if the
    /// range vector holds series, and a scalar grid if it was made from a scalar.
    const bool has_group = (range_argument.store_method == StoreMethod::VECTOR_GRID) || (range_argument.store_method == StoreMethod::RAW_DATA);

    SelectQueryBuilder builder;

    if (has_group)
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    ASTPtr regression = addParametersToAggregateFunction(
        makeASTFunction("timeSeriesLinearRegressionToGrid", std::move(aggregate_function_arguments)),
        timeSeriesTimestampToAST(aggregation_range.start_time, context.result_timestamp_type),
        timeSeriesTimestampToAST(aggregation_range.end_time, context.result_timestamp_type),
        timeSeriesDurationToAST(aggregation_range.step, context.result_timestamp_type),
        timeSeriesDurationToAST(window, context.result_timestamp_type));

    std::optional<HorizonShift> horizon_shift;
    if (fixed_at_node)
    {
        /// The line fitted to the frozen window is the same at every step, so the single result of the aggregation
        /// is repeated over the grid.
        regression = repeatFixedAtResultOverGrid(std::move(regression), aggregation_range, result_grid_size);

        /// A fixed @ freezes only the sample window, the prediction is still made from the evaluation time: PromQL evaluates
        /// `predict_linear` at every step even if all its arguments are fixed (see AtModifierUnsafeFunctions in Prometheus).
        /// The fit is linear, so predicting further ahead by the distance from the frozen timestamp to the step moves the
        /// origin there exactly.
        horizon_shift = HorizonShift{
            .shift_at_start = DecimalUtils::convertTo<Float64>(
                DurationType{start_time.value - aggregation_range.start_time.value}, context.result_timestamp_scale),
            .step_in_seconds = DecimalUtils::convertTo<Float64>(step, context.result_timestamp_scale),
            .grid_size = result_grid_size};
    }

    ASTPtr aggregate_values = makePredictions(std::move(regression), std::move(prediction_offset), horizon_shift);

    builder.select_list.push_back(std::move(aggregate_values));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    if (has_group)
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    if (range_argument.select_query)
    {
        auto & subqueries = context.subqueries;
        subqueries.emplace_back(subqueries.size(), std::move(range_argument.select_query), SQLSubqueryType::TABLE);
        builder.from_table = subqueries.back().name;
    }

    SQLQueryPiece res = range_argument;
    res.store_method = has_group ? StoreMethod::VECTOR_GRID : StoreMethod::SCALAR_GRID;
    res.scalar_value = {};
    res.node = function_node;

    res.select_query = builder.getSelectQuery();
    res.type = ResultType::INSTANT_VECTOR;
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;

    /// `predict_linear` always drops the metric name (PromQL: function outputs have no `__name__`).
    if (has_group)
        res = dropMetricName(std::move(res), context);

    return res;
}

}
