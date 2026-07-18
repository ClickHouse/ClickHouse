#include <Processors/QueryPlan/numbersLikeUtils.h>

#include <algorithm>

#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/SizeLimits.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 max_rows_to_read;
extern const SettingsUInt64 max_rows_to_read_leaf;
extern const SettingsOverflowMode read_overflow_mode;
extern const SettingsOverflowMode read_overflow_mode_leaf;
}

namespace ErrorCodes
{
extern const int TOO_MANY_ROWS;
}

namespace NumbersLikeUtils
{

ExtractedRanges extractRanges(const KeyCondition & condition)
{
    ExtractedRanges result;

    if (condition.extractPlainRanges(result.ranges))
    {
        result.kind = ExtractedRanges::Kind::ExactRanges;
        return result;
    }

    result.kind = ExtractedRanges::Kind::ConservativeRanges;
    result.ranges = condition.extractBounds();
    return result;
}

void applyQueryLimit(std::optional<UInt64> & effective_limit, const std::optional<size_t> & query_limit)
{
    if (!query_limit)
        return;

    const UInt64 query_limit_64 = static_cast<UInt64>(*query_limit);
    effective_limit = std::min(effective_limit.value_or(query_limit_64), query_limit_64);
}

void addNullSource(Pipe & pipe, SharedHeader header)
{
    pipe.addSource(std::make_shared<NullSource>(std::move(header)));
}

namespace
{

/// The function name is canonicalized so the case-insensitive alias `unnest` is also caught
/// even when function names are not normalized (`normalize_function_names = 0`).
bool astContainsArrayJoinFunction(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * function = ast->as<ASTFunction>())
        if (getFunctionCanonicalNameIfAny(function->name) == "arrayJoin")
            return true;
    for (const auto & child : ast->children)
        if (!child->as<ASTSelectQuery>() && astContainsArrayJoinFunction(child))
            return true;
    return false;
}

/// Whether the AST subtree contains a call to a stateful function (`IFunctionBase::isStateful`,
/// e.g. `neighbor`, `runningAccumulate`, `logTrace`), without descending into nested subqueries.
/// Mirrors `selectListHasStatefulFunction` in `InterpreterSelectQuery.cpp`.
bool astContainsStatefulFunction(const ASTPtr & ast, const ContextPtr & context)
{
    if (!ast)
        return false;
    if (const auto * function = ast->as<ASTFunction>())
    {
        const auto function_resolver = FunctionFactory::instance().tryGet(function->name, context);
        if (function_resolver && function_resolver->isStateful())
            return true;
    }
    for (const auto & child : ast->children)
        if (!child->as<ASTSelectQuery>() && astContainsStatefulFunction(child, context))
            return true;
    return false;
}

bool shouldPushdownLimit(const SelectQueryInfo & query_info, const InterpreterSelectQuery::LimitInfo & lim_info, const ContextPtr & context)
{
    /// Reject negative, fractional, and zero limits for pushdown
    if (lim_info.is_limit_length_negative
        || lim_info.fractional_limit > 0
        || lim_info.fractional_offset > 0
        || lim_info.limit_length == 0)
        return false;

    chassert(query_info.query);

    const auto & query = query_info.query->as<ASTSelectQuery &>();

    /// `arrayJoin` (function or `ARRAY JOIN` clause) changes row cardinality after the
    /// source has run. Pushing the outer `LIMIT` into the source would truncate input
    /// rows BEFORE expansion, silently dropping output rows when arrays are empty or
    /// producing wrong rows when arrays expand. See issue #82279 and the sibling guards
    /// in the query-plan optimizer passes (`liftUpFunctions`, `optimizeLazyMaterialization`,
    /// `optimizeTopK`, `topKThroughJoin`, `pushLimitByIntoSort`,
    /// `optimizePrimaryKeyConditionAndLimit`).
    ///
    /// The `arrayJoin` function call may appear in the SELECT clause or in a filter
    /// (`WHERE`/`PREWHERE`, e.g. `WHERE arrayJoin(...) >= 0` or a `WITH` alias referenced only
    /// there), while the `ARRAY JOIN` clause is stored separately in `arrayJoinExpressionList()`
    /// (the clause itself is already an array-join operation, regardless of what its expressions
    /// contain). All forms must reject pushdown.
    if (astContainsArrayJoinFunction(query.select())
        || astContainsArrayJoinFunction(query.where())
        || astContainsArrayJoinFunction(query.prewhere()))
        return false;
    if (query.arrayJoinExpressionList().first)
        return false;

    /// A stateful function (e.g. `neighbor`, `runningAccumulate`, `logTrace`) gives block- and
    /// data-order dependent results and side effects, so it must see the same input rows it would
    /// see without the optimization. Capping the source to `limit + offset` rows would truncate
    /// its input. See the sibling guards in `InterpreterSelectQuery::maxBlockSizeByLimit` and
    /// `mainQueryNodeBlockSizeByLimit`. Like `arrayJoin`, such a function may also sit in a filter
    /// (`WHERE`/`PREWHERE`), not only in the SELECT clause.
    if (astContainsStatefulFunction(query.select(), context)
        || astContainsStatefulFunction(query.where(), context)
        || astContainsStatefulFunction(query.prewhere(), context))
        return false;

    /// Just ignore some minor cases, such as:
    ///     select * from system.numbers order by number asc limit 10
    return !query.distinct
        && !query.limitBy()
        && !query_info.has_order_by
        && !query_info.need_aggregate
        /// For the analyzer, window will be deleted from AST, so we should not use query.window()
        && !query_info.has_window
        && !query_info.additional_filter_ast
        && !query.limit_with_ties;
}

}

std::optional<size_t> getLimitFromQueryInfo(const SelectQueryInfo & query_info, const ContextPtr & context)
{
    if (!query_info.query)
        return {};

    const auto lim_info = InterpreterSelectQuery::getLimitLengthAndOffset(query_info.query->as<ASTSelectQuery &>(), context);

    if (!shouldPushdownLimit(query_info, lim_info, context))
        return {};

    return lim_info.limit_length + lim_info.limit_offset;
}

void checkLimits(const Settings & settings, size_t rows)
{
    if (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read])
    {
        const auto limits = SizeLimits(settings[Setting::max_rows_to_read], 0, settings[Setting::read_overflow_mode]);
        limits.check(rows, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
    }

    if (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf])
    {
        const auto leaf_limits = SizeLimits(settings[Setting::max_rows_to_read_leaf], 0, settings[Setting::read_overflow_mode_leaf]);
        leaf_limits.check(rows, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
    }
}

}

}
