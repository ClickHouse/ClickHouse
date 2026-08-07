#include <Analyzer/Passes/OptimizeTrivialGroupByLimitPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsBool optimize_trivial_group_by_limit_query;
}

namespace
{

/// Reads LIMIT/OFFSET as `UInt64`. Analyzer keeps negative or fractional
/// values as `Int64`/`Float64`, so `safeGet<UInt64>` would throw on them.
/// Returns `std::nullopt` for negative or fractional values so the caller
/// can skip the optimization in those cases.
std::optional<UInt64> tryGetNonNegativeUInt64(const Field & field)
{
    const Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        return std::nullopt;
    return converted.safeGet<UInt64>();
}

}

void OptimizeTrivialGroupByLimitPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    if (!settings[Setting::optimize_trivial_group_by_limit_query])
        return;

    auto * query = query_tree_node->as<QueryNode>();
    if (!query || !query->hasGroupBy() || !query->hasLimit() || query->hasHaving() || query->hasOrderBy() || query->hasWindow()
        || query->hasLimitBy() || query->isGroupByWithTotals() || query->isGroupByWithRollup() || query->isGroupByWithCube()
        || query->isGroupByWithGroupingSets() || hasAggregateFunctionNodes(query->getProjectionNode()))
        return;

    /// `group_by_overflow_mode` controls what happens when `max_rows_to_group_by` is exceeded.
    /// The optimization only makes sense in `ANY` mode (keep first N keys, drop the rest);
    /// `THROW` would turn the optimization into a spurious exception, `BREAK` aborts the query.
    /// Don't change the mode if the user has set it explicitly to something non-`ANY` —
    /// they have an explicit contract that the optimization would silently break.
    const bool mode_is_any = settings[Setting::group_by_overflow_mode] == OverflowMode::ANY;
    const bool mode_is_changed = settings[Setting::group_by_overflow_mode].changed;
    if (!mode_is_any && mode_is_changed)
        return;

    auto limit = tryGetNonNegativeUInt64(query->getLimit()->as<ConstantNode &>().getValue());
    if (!limit)
        return;
    UInt64 offset = 0;
    if (query->hasOffset())
    {
        auto maybe_offset = tryGetNonNegativeUInt64(query->getOffset()->as<ConstantNode &>().getValue());
        if (!maybe_offset)
            return;
        offset = *maybe_offset;
    }
    UInt64 max_rows = 0;
    if (common::addOverflow(*limit, offset, max_rows))
        return;

    /// `max_rows_to_group_by = 0` means "no cap" in ClickHouse, so applying the optimization
    /// for `LIMIT 0` (or `LIMIT + OFFSET = 0`) would silently remove the user's explicit cap.
    /// The query also returns no rows regardless, so the optimization buys nothing.
    if (max_rows == 0)
        return;

    /// If the user has already set `max_rows_to_group_by`, we only apply the optimization
    /// when our derived value is strictly smaller — otherwise the user's setting is tighter
    /// and ours would be a no-op. When the user has a tighter throw/break contract, we'd
    /// also break their semantics (already guarded by the `mode_is_changed` check above).
    const UInt64 user_max_rows = settings[Setting::max_rows_to_group_by];
    if (user_max_rows != 0 && user_max_rows <= max_rows)
        return;

    auto & mutable_context = query->getMutableContext();
    mutable_context->setSetting("max_rows_to_group_by", max_rows);
    if (!mode_is_any)
        mutable_context->setSetting("group_by_overflow_mode", Field("any"));
}

}
