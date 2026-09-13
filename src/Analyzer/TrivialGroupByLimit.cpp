#include <Analyzer/TrivialGroupByLimit.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/convertColumnToType.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace Setting
{
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsBool optimize_trivial_group_by_limit_query;
}

namespace
{

/// Reads LIMIT/OFFSET as `UInt64`. Analyzer keeps negative or fractional
/// values as `Int64`/`Float64`, so `safeGet<UInt64>` would throw on them.
/// Returns `std::nullopt` for negative or fractional values so the caller
/// can skip the optimization in those cases.
std::optional<UInt64> tryGetNonNegativeUInt64(const ConstantNode * node)
{
    if (!node)
        return std::nullopt;
    ColumnPtr converted = convertColumnToTypeOrNull(*node->getColumn(), node->getResultType(), std::make_shared<DataTypeUInt64>());
    if (!converted)
        return std::nullopt;
    return converted->getUInt(0);
}

/// `arrayJoin` can be spelled through its case-insensitive alias `unnest`, and with
/// `normalize_function_names = 0` the alias reaches the query tree unchanged, so a literal
/// name comparison (as in `hasFunctionNode`) would miss it. Compare canonical names instead.
class CheckArrayJoinExistsVisitor : public ConstInDepthQueryTreeVisitor<CheckArrayJoinExistsVisitor>
{
public:
    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (has_array_join)
            return;

        const auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        has_array_join = getFunctionCanonicalNameIfAny(function_node->getFunctionName()) == "arrayJoin";
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node) const
    {
        if (has_array_join)
            return false;

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

    bool hasArrayJoin() const
    {
        return has_array_join;
    }

private:
    bool has_array_join = false;
};

bool hasArrayJoinFunctionNode(const QueryTreeNodePtr & node)
{
    CheckArrayJoinExistsVisitor visitor;
    visitor.visit(node);
    return visitor.hasArrayJoin();
}

}

std::optional<UInt64> getTrivialGroupByLimit(const QueryNode & query, const Settings & settings)
{
    if (!settings[Setting::optimize_trivial_group_by_limit_query])
        return std::nullopt;

    if (!query.hasGroupBy() || !query.hasLimit() || query.hasHaving() || query.hasOrderBy() || query.hasWindow()
        || query.hasQualify() || query.hasLimitBy() || query.isDistinct() || query.isGroupByWithTotals()
        || query.isGroupByWithRollup() || query.isGroupByWithCube() || query.isGroupByWithGroupingSets())
        return std::nullopt;

    /// Window functions and `arrayJoin` in the projection consume the aggregated rows after
    /// GROUP BY, so the produced groups are not simply cut by LIMIT and keeping only the first
    /// `LIMIT + OFFSET` groups can change the result:
    /// - a window function is evaluated over all groups (`count() OVER ()` counts them);
    /// - `arrayJoin` can expand or drop rows, so `LIMIT + OFFSET` groups may produce fewer
    ///   rows than the LIMIT while more groups exist.
    /// `DISTINCT` and `QUALIFY` (checked above) collapse and filter the groups in the same way.
    if (hasWindowFunctionNodes(query.getProjectionNode()) || hasArrayJoinFunctionNode(query.getProjectionNode()))
        return std::nullopt;

    /// `group_by_overflow_mode` controls what happens when `max_rows_to_group_by` is exceeded.
    /// The optimization only makes sense in `ANY` mode (keep first N keys, drop the rest);
    /// `THROW` would turn the optimization into a spurious exception, `BREAK` aborts the query.
    /// Don't change the mode if the user has set it explicitly to something non-`ANY` —
    /// they have an explicit contract that the optimization would silently break.
    const bool mode_is_any = settings[Setting::group_by_overflow_mode] == OverflowMode::ANY;
    const bool mode_is_changed = settings[Setting::group_by_overflow_mode].changed;
    if (!mode_is_any && mode_is_changed)
        return std::nullopt;

    auto limit = tryGetNonNegativeUInt64(query.getLimit()->as<ConstantNode>());
    if (!limit)
        return std::nullopt;
    UInt64 offset = 0;
    if (query.hasOffset())
    {
        auto maybe_offset = tryGetNonNegativeUInt64(query.getOffset()->as<ConstantNode>());
        if (!maybe_offset)
            return std::nullopt;
        offset = *maybe_offset;
    }
    UInt64 max_rows = 0;
    if (common::addOverflow(*limit, offset, max_rows))
        return std::nullopt;

    /// `max_rows_to_group_by = 0` means "no cap" in ClickHouse, so applying the optimization
    /// for `LIMIT 0` (or `LIMIT + OFFSET = 0`) would silently remove the user's explicit cap.
    /// The query also returns no rows regardless, so the optimization buys nothing.
    if (max_rows == 0)
        return std::nullopt;

    return max_rows;
}

}
