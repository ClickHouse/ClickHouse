#include <Analyzer/Passes/GroupByLimitToDistinctPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/convertFieldToType.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool optimize_group_by_limit_to_distinct;
    extern const SettingsUInt64 optimize_group_by_limit_to_distinct_max_limit;
    extern const SettingsUInt64 max_bytes_in_distinct;
    extern const SettingsUInt64 max_rows_in_distinct;
    extern const SettingsUInt64 max_rows_to_group_by;
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

class GroupByLimitToDistinctVisitor : public InDepthQueryTreeVisitorWithContext<GroupByLimitToDistinctVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<GroupByLimitToDistinctVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        const auto & settings = getSettings();
        if (!settings[Setting::optimize_group_by_limit_to_distinct])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        /// Without LIMIT the rewrite buys nothing: the whole input has to be processed anyway.
        if (!query->hasGroupBy() || !query->hasLimit())
            return;

        /// The rewrite pays off only for small limits. When the limit is large — or the key
        /// cardinality is below the limit, so the read never terminates early — the whole input
        /// goes through DISTINCT, which is weaker than aggregation on high-cardinality data:
        /// the final DistinctTransform runs on a single stream, and the DISTINCT set cannot
        /// spill to disk the way aggregation can. Capping LIMIT + OFFSET bounds the distinct
        /// set by min(cardinality, LIMIT + OFFSET) <= the threshold, so the worst case (input
        /// fully consumed) costs the same as the aggregation it replaces: a full read with
        /// per-row probes into an equally small hash table.
        const auto * limit_constant = query->getLimit()->as<ConstantNode>();
        if (!limit_constant)
            return;
        auto limit = tryGetNonNegativeUInt64(limit_constant->getValue());
        if (!limit)
            return;

        UInt64 offset = 0;
        if (query->hasOffset())
        {
            const auto * offset_constant = query->getOffset()->as<ConstantNode>();
            if (!offset_constant)
                return;
            auto maybe_offset = tryGetNonNegativeUInt64(offset_constant->getValue());
            if (!maybe_offset)
                return;
            offset = *maybe_offset;
        }

        UInt64 required_distinct_rows = 0;
        if (common::addOverflow(*limit, offset, required_distinct_rows))
            return;

        /// LIMIT 0 returns no rows regardless, nothing to optimize.
        if (required_distinct_rows == 0 || required_distinct_rows > settings[Setting::optimize_group_by_limit_to_distinct_max_limit])
            return;

        if (query->hasHaving() || query->hasOrderBy() || query->hasWindow() || query->hasQualify() || query->hasLimitBy()
            || query->isLimitWithTies() || query->isGroupByWithTotals() || query->isGroupByWithRollup() || query->isGroupByWithCube()
            || query->isGroupByWithGroupingSets())
            return;

        /// group_by_use_nulls affects only the GROUP BY modifiers excluded above,
        /// but be conservative, same as OptimizeGroupByFunctionKeysPass.
        if (settings[Setting::group_by_use_nulls])
            return;

        /// max_rows_to_group_by with a non-throw overflow mode makes GROUP BY return a partial
        /// result, and after the rewrite the cap would no longer apply. Conversely, the DISTINCT
        /// limits would start applying to a query that did not have DISTINCT. Preserve the user's
        /// explicit contracts by skipping the rewrite when any of these limits is set.
        if (settings[Setting::max_rows_to_group_by] != 0 || settings[Setting::max_rows_in_distinct] != 0
            || settings[Setting::max_bytes_in_distinct] != 0)
            return;

        if (hasAggregateFunctionNodes(query->getProjectionNode()))
            return;

        /// The rewrite is valid only when the projection and the GROUP BY keys are the same set
        /// of expressions: DISTINCT deduplicates the projection columns, so a key that is not
        /// projected (SELECT a ... GROUP BY a, b produces a row per (a, b) pair, possibly with
        /// duplicate values of a) would change the result, and a projected expression that is
        /// not a key could be non-deterministic (making the projected rows distinct in a way
        /// the groups are not).
        const auto & projection_nodes = query->getProjection().getNodes();
        const auto & group_by_nodes = query->getGroupBy().getNodes();

        auto is_contained_in = [](const QueryTreeNodePtr & needle, const QueryTreeNodes & haystack)
        {
            for (const auto & candidate : haystack)
                if (needle->isEqual(*candidate, {.compare_aliases = false}))
                    return true;
            return false;
        };

        for (const auto & projection_node : projection_nodes)
            if (!is_contained_in(projection_node, group_by_nodes))
                return;

        for (const auto & group_by_node : group_by_nodes)
            if (!is_contained_in(group_by_node, projection_nodes))
                return;

        query->setIsDistinct(true);
        query->setIsGroupByAll(false);
        query->getGroupBy().getNodes().clear();
    }
};

}

void GroupByLimitToDistinctPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    GroupByLimitToDistinctVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
