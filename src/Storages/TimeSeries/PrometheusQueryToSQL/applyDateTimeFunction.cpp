#include <Storages/TimeSeries/PrometheusQueryToSQL/applyDateTimeFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionScalar.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionVector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionTime.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a date/time function.
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() > 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 0 or 1 arguments, but was called with {} arguments",
                            function_name, arguments.size());
        }

        if (arguments.empty())
            return;

        const auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }

    using TransformASTFunc = ASTPtr (*)(ASTPtr t);

    struct ImplInfo
    {
        TransformASTFunc transform_ast;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"day_of_week",
             {
                 /// Returned values should be from 0 to 6, where 0 means Sunday.
                 [](ASTPtr t) -> ASTPtr
                 { return makeASTFunction("toDayOfWeek", std::move(t), /* mode = */ make_intrusive<ASTLiteral>(2u)); },
             }},

            {"day_of_month",
             {
                 /// Returned values should be from 1 to 31.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDayOfMonth", std::move(t)); },
             }},

            {"days_in_month",
             {
                 /// Returned values should be from 28 to 31.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDaysInMonth", std::move(t)); },
             }},

            {"day_of_year",
             {
                 /// Returned values should be from 1 to 365 for non-leap years, and 1 to 366 in leap years.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDayOfYear", std::move(t)); },
             }},

            {"minute",
             {
                 /// Returned values should be from 0 to 59.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toMinute", std::move(t)); },
             }},

            {"hour",
             {
                 /// Returned values should be from 0 to 23.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toHour", std::move(t)); },
             }},

            {"month",
             {
                 /// Returned values should be from 1 to 12, where 1 means January.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toMonth", std::move(t)); },
             }},

            {"year",
             {
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toYear", std::move(t)); },
             }},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }

    /// Finds the `time()` call reachable from `node` after peeling off any number of `scalar(...)`, `vector(...)`,
    /// unary `+...`, parentheses, and `Offset` (`@ <timestamp>` / `offset <duration>`) wrappers. All of these are
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
        if (node->node_type == NodeType::ParenExpression)
        {
            const auto * paren = static_cast<const PrometheusQueryTree::ParenExpression *>(node);
            return findTimeCallThroughScalarVectorWrappers(paren->getExpression());
        }

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
}


bool isDateTimeFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyDateTimeFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_node, arguments, context);

    if (arguments.empty())
    {
        /// A date/time function called without arguments acts as if it was called with `vector(time())`.
        /// Use makeTimeQueryPieceNative() here (instead of makeTimeQueryPiece(), which time() itself uses) to keep
        /// the evaluation time in `context.timestamp_data_type` (native DateTime64 precision) through the
        /// toDateTime64() conversion below, instead of `context.scalar_data_type` (which can be Float32 and, for a
        /// range query, would round the evaluation-time array to ~128-second granularity at today's epoch
        /// magnitude before the calendar component is even extracted).
        auto time_argument = makeTimeQueryPieceNative(function_node, context);
        time_argument.type = ResultType::INSTANT_VECTOR;
        arguments.push_back(std::move(time_argument));
    }
    else if (const auto * time_node = findTimeCallThroughScalarVectorWrappers(function_node->getArguments()[0]))
    {
        /// The argument is `time()`, possibly wrapped in any nesting of `scalar(...)`/`vector(...)`/unary `+...`
        /// (e.g. `vector(time())`, `scalar(vector(time()))`, `vector(scalar(vector(time())))`). The PromQL spec
        /// says a 0-argument call like `f()` is equivalent to `f(vector(time()))`, and all of the wrappers above
        /// are value-preserving, so every one of these spellings should agree with `f()`. The generic conversion
        /// path already ran for this argument (fromFunctionTime() -> makeTimeQueryPiece(), then possibly
        /// applyFunctionScalar()/applyFunctionVector()/applyUnaryOperator()/applyOffset() passing it through
        /// unchanged), which represents the evaluation time via `context.scalar_data_type` - the same
        /// Float32-losing-precision path described above. Rebuild the argument with makeTimeQueryPieceNative()
        /// instead, exactly like the 0-argument branch above, so that all of these spellings and `f()` always agree.
        auto time_argument = makeTimeQueryPieceNative(time_node, context);
        time_argument.type = ResultType::INSTANT_VECTOR;
        arguments[0] = std::move(time_argument);
    }

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        /// f(toDateTime64(x, 0, 'UTC'))::scalar_data_type
        chassert(args.size() == 1);
        ASTPtr x = std::move(args[0]);
        return timeSeriesScalarASTCast(
            (impl_info->transform_ast)(
                makeASTFunction("toDateTime64", std::move(x), make_intrusive<ASTLiteral>(0u), make_intrusive<ASTLiteral>("UTC"))),
            context.scalar_data_type);
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
