#include <Analyzer/Passes/GroupByLimitToDistinctPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace Setting
{
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsUInt64 max_bytes_before_external_group_by;
    extern const SettingsDouble max_bytes_ratio_before_external_group_by;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsBool optimize_group_by_limit_to_distinct;
    extern const SettingsUInt64 optimize_group_by_limit_to_distinct_max_limit;
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

        /// `max_rows_to_group_by` caps the number of groups the aggregation may build and
        /// `group_by_overflow_mode` decides what happens when the cap is hit (throw, stop, or keep
        /// the first N groups). Dropping the aggregation drops that contract, so back off whenever
        /// the user's cap can bind on this query:
        ///   * a cap not above LIMIT + OFFSET would truncate (or reject) the very rows the query
        ///     asks for, so the rewritten query would return more rows than the original;
        ///   * an explicitly chosen non-`any` mode means the user wants the query to fail or stop
        ///     rather than to silently return an arbitrary subset of the groups.
        /// This mirrors OptimizeTrivialGroupByLimitPass, which backs off in the same two cases.
        /// For a cap above LIMIT + OFFSET with the default mode, that pass — enabled by default —
        /// already lowers the cap to LIMIT + OFFSET and switches the mode to `any` for exactly this
        /// query shape, so the overflow can no longer be observed there either.
        const UInt64 max_rows_to_group_by = settings[Setting::max_rows_to_group_by];
        if (max_rows_to_group_by != 0)
        {
            if (max_rows_to_group_by <= required_distinct_rows)
                return;

            const bool mode_is_any = settings[Setting::group_by_overflow_mode] == OverflowMode::ANY;
            const bool mode_is_changed = settings[Setting::group_by_overflow_mode].changed;
            if (!mode_is_any && mode_is_changed)
                return;
        }

        if (query->hasHaving() || query->hasOrderBy() || query->hasWindow() || query->hasQualify() || query->hasLimitBy()
            || query->isLimitWithTies() || query->isGroupByWithTotals() || query->isGroupByWithRollup() || query->isGroupByWithCube()
            || query->isGroupByWithGroupingSets())
            return;

        /// group_by_use_nulls affects only the GROUP BY modifiers excluded above,
        /// but be conservative, same as OptimizeGroupByFunctionKeysPass.
        if (settings[Setting::group_by_use_nulls])
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

        /// Bounding the number of rows in the distinct set does not bound its memory: for keys of
        /// unbounded width (String, Array, Map, ...) even LIMIT + OFFSET rows can hold arbitrarily
        /// many bytes. That matters because aggregation can spill to disk once it crosses the
        /// external-aggregation threshold while the DISTINCT set cannot, so a query that today
        /// completes via external aggregation could start throwing MEMORY_LIMIT_EXCEEDED.
        /// Rewrite only when the byte footprint is provably bounded — the key types have a maximum
        /// size and the worst case fits in an explicitly configured threshold — or when external
        /// aggregation is disabled for this query, in which case aggregation holds the very same
        /// keys in memory and the two plans have the same footprint.
        const UInt64 max_bytes_before_external_group_by = settings[Setting::max_bytes_before_external_group_by];
        const bool aggregation_can_spill
            = max_bytes_before_external_group_by != 0 || settings[Setting::max_bytes_ratio_before_external_group_by] != 0.;
        if (aggregation_can_spill)
        {
            UInt64 max_bytes_per_row = 0;
            for (const auto & group_by_node : group_by_nodes)
            {
                const auto & key_type = group_by_node->getResultType();
                if (!key_type || !key_type->haveMaximumSizeOfValue())
                    return;
                if (common::addOverflow(max_bytes_per_row, UInt64(key_type->getMaximumSizeOfValueInMemory()), max_bytes_per_row))
                    return;
            }

            UInt64 max_distinct_set_bytes = 0;
            if (common::mulOverflow(max_bytes_per_row, required_distinct_rows, max_distinct_set_bytes))
                return;
            if (max_bytes_before_external_group_by != 0 && max_distinct_set_bytes > max_bytes_before_external_group_by)
                return;
        }

        const bool had_distinct = query->isDistinct();

        query->setIsDistinct(true);
        query->setIsGroupByAll(false);
        query->getGroupBy().getNodes().clear();

        /// The DISTINCT set size limits (commonly set as global sanity limits, e.g. in the CI
        /// test configs) must not start applying to a query the user did not write DISTINCT in:
        /// the original query was not subject to them, and the rewritten query's distinct set is
        /// bounded by LIMIT + OFFSET <= optimize_group_by_limit_to_distinct_max_limit rows anyway.
        /// When the query already had DISTINCT on top of GROUP BY, the user's limits keep applying.
        /// max_rows_to_group_by is not transferred either: it stops applying together with the
        /// aggregation it guards, and the cases where it could still bind are excluded above.
        if (!had_distinct)
        {
            auto & mutable_context = query->getMutableContext();
            mutable_context->setSetting("max_rows_in_distinct", UInt64(0));
            mutable_context->setSetting("max_bytes_in_distinct", UInt64(0));
        }
    }
};

}

void GroupByLimitToDistinctPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    GroupByLimitToDistinctVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
