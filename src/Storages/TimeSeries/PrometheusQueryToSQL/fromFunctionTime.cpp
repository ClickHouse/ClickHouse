#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionTime.h>

#include <Core/DecimalFunctions.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionScalar.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionVector.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece makeTimeQueryPiece(const PrometheusQueryTree::Node * node, ConverterContext & context)
{
    auto node_range = context.node_range_getter.get(node);
    if (node_range.empty())
        return SQLQueryPiece{node, ResultType::SCALAR, StoreMethod::EMPTY};

    if (node_range.start_time == node_range.end_time)
    {
        /// Single evaluation time, so we use StoreMethod::CONST_SCALAR.
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;
        res.scalar_value = DecimalUtils::convertTo<Float64>(node_range.start_time, context.timestamp_scale);
        return res;
    }
    else
    {
        /// Range of evaluation times (e.g. "time()[10m:1m]"), so we use StoreMethod::SCALAR_GRID.
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::SCALAR_GRID};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;

        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction(
            "CAST",
            makeASTFunction(
                "timeSeriesRange",
                timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(node_range.step, context.timestamp_data_type)),
            make_intrusive<ASTLiteral>(fmt::format("Array({})", context.scalar_data_type->getName()))));

        builder.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = builder.getSelectQuery();

        return res;
    }
}


SQLQueryPiece makeTimeQueryPieceNative(const PrometheusQueryTree::Node * node, ConverterContext & context)
{
    auto node_range = context.node_range_getter.get(node);
    if (node_range.empty())
        return SQLQueryPiece{node, ResultType::SCALAR, StoreMethod::EMPTY};

    if (node_range.start_time == node_range.end_time)
    {
        /// Single evaluation time. Unlike makeTimeQueryPiece()'s CONST_SCALAR branch, return it via a one-row
        /// subquery (StoreMethod::SINGLE_SCALAR) keeping the value typed as `context.timestamp_data_type`,
        /// instead of converting it to a Float64 scalar_value (which a caller would then have to cast to
        /// `context.scalar_data_type`, losing precision if that's Float32).
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::SINGLE_SCALAR};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;

        SelectQueryBuilder builder;
        builder.select_list.push_back(timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type));
        builder.select_list.back()->setAlias(ColumnNames::Value);
        res.select_query = builder.getSelectQuery();

        return res;
    }
    else
    {
        /// Range of evaluation times. Unlike makeTimeQueryPiece()'s SCALAR_GRID branch, don't cast the result of
        /// timeSeriesRange() down to Array(scalar_data_type); keep its native return type Array(timestamp_data_type)
        /// so no precision is lost before a caller extracts a calendar component from each timestamp.
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::SCALAR_GRID};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;

        SelectQueryBuilder builder;
        builder.select_list.push_back(makeASTFunction(
            "timeSeriesRange",
            timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(node_range.step, context.timestamp_data_type)));

        builder.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = builder.getSelectQuery();

        return res;
    }
}


/// Finds the `time()` call reachable from `node` after peeling off any number of `scalar(...)`, `vector(...)`,
/// unary `+...`, and `Offset` (`@ <timestamp>` / `offset <duration>`) wrappers. All of these are
/// value-preserving no-ops in the generic conversion path: applyFunctionScalar()'s
/// CONST_SCALAR/SINGLE_SCALAR/SCALAR_GRID cases, applyFunctionVector(), applyUnaryOperator()'s '+' case, and
/// applyOffset()'s offsetEvaluationTime()/setEvaluationTime() (for those same store methods) each return
/// their argument's SQLQueryPiece unchanged (aside from `type`/`node`/`start_time`/`end_time`/`step`
/// bookkeeping - never touching `scalar_value`/`select_query`), so any nesting of these around `time()` - e.g.
/// `vector(time())`, `scalar(vector(time()))`, `vector(scalar(vector(time())))`, `+time()` - carries the exact
/// same (possibly Float32-lossy) underlying value. Skipping the `Offset` node here at conversion time would be
/// safe: `NodeEvaluationRangeGetter` pre-computes each node's evaluation range in a separate upfront
/// AST-walking pass (before any conversion), and for an `Offset` node it already applies the `@`/`offset`
/// adjustment to the range it assigns to the *inner* expression (see NodeEvaluationRangeGetter.cpp), so looking
/// up the range for the innermost `time()` node directly would still yield the correctly shifted
/// start_time/end_time. NOTE: as of this writing, the `Offset` branch below is unreachable in practice - per
/// the PromQL grammar (contrib/antlr4-grammars/promql/PromQLParser.g4) and its ANTLR visitor
/// (PrometheusQueryParsingUtil-antlr.cpp), an `Offset` node is only ever constructed directly around an
/// `InstantSelector`, `RangeSelector`, or `Subquery` node - never directly around a `Function` or
/// `UnaryOperator` node - so `@`/`offset` can't syntactically attach directly to `time()`/`scalar(...)`/
/// `vector(...)`/unary `+` (e.g. `vector(time()) @ 123` fails to parse). The branch is kept anyway for
/// defensive forward-compatibility (e.g. if the grammar is ever relaxed) and is a verified no-op for every
/// currently-reachable AST, since it only recurses into cases the pre-existing checks already reject.
/// Returns nullptr if `node` isn't (possibly wrapped) exactly a bare `time()` call.
const PrometheusQueryTree::Function * findTimeCallThroughScalarVectorWrappers(const Node * node)
{
    if (node->node_type == NodeType::UnaryOperator)
    {
        const auto * unary_operator = static_cast<const PrometheusQueryTree::UnaryOperator *>(node);
        if (unary_operator->operator_name != "+")
            return nullptr;

        return findTimeCallThroughScalarVectorWrappers(unary_operator->getArgument());
    }

    if (node->node_type == NodeType::Offset)
    {
        const auto * offset = static_cast<const PrometheusQueryTree::Offset *>(node);
        return findTimeCallThroughScalarVectorWrappers(offset->getExpression());
    }

    if (node->node_type != NodeType::Function)
        return nullptr;

    const auto * function = static_cast<const PrometheusQueryTree::Function *>(node);

    if (isFunctionTime(function->function_name))
        return function->getArguments().empty() ? function : nullptr;

    if ((isFunctionScalar(function->function_name) || isFunctionVector(function->function_name))
        && (function->getArguments().size() == 1))
        return findTimeCallThroughScalarVectorWrappers(function->getArguments()[0]);

    return nullptr;
}


namespace
{
    /// Whether `node`'s subtree calls `time()` anywhere, i.e. whether its value can be of evaluation-time magnitude.
    bool containsTimeCall(const Node * node)
    {
        if (node->node_type == NodeType::Function)
        {
            const auto * function = static_cast<const PrometheusQueryTree::Function *>(node);
            if (isFunctionTime(function->function_name) && function->getArguments().empty())
                return true;
        }

        for (const auto * child : node->children)
        {
            if (containsTimeCall(child))
                return true;
        }

        return false;
    }
}


SQLQueryPiece makeVaryingScalarPrecisionSafe(
    std::string_view function_name, const Node * argument_node, SQLQueryPiece && argument, ConverterContext & context)
{
    /// Only a grid of per-evaluation-step values goes through `Array(context.scalar_data_type)`; the constant and
    /// single-row store methods carry a Float64 `scalar_value` or the table's own values, which is what they mean.
    if (argument.store_method != StoreMethod::SCALAR_GRID)
        return std::move(argument);

    /// A (possibly wrapped) bare `time()` can be rebuilt at the timestamp type's own precision, so the caller's
    /// cast to Array(Float64) sees exact instants instead of ones already rounded to `context.scalar_data_type`.
    if (const auto * time_node = findTimeCallThroughScalarVectorWrappers(argument_node))
    {
        auto native = makeTimeQueryPieceNative(time_node, context);
        /// SCALAR_GRID means the node spans several evaluation times, which is the same branch both builders take.
        chassert(native.store_method == StoreMethod::SCALAR_GRID);
        native.node = argument.node;
        return native;
    }

    if (!WhichDataType{context.scalar_data_type}.isFloat64() && containsTimeCall(argument_node))
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' cannot use expression {} as its varying scalar argument on a TimeSeries table "
                        "whose value type is {}: the evaluation time would be rounded to that type before the "
                        "function sees it (Float32 resolves only ~128 seconds at current timestamps). "
                        "Pass time() directly, or use a Float64 value column",
                        function_name, getPromQLText(argument, context), context.scalar_data_type->getName());
    }

    return std::move(argument);
}


SQLQueryPiece fromFunctionTime(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionTime(function_name));

    if (!arguments.empty())
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects no arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

    return makeTimeQueryPiece(function_node, context);
}

}
