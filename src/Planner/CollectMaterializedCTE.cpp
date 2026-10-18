#include <Planner/CollectMaterializedCTE.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/traverseQueryTree.h>
#include <Interpreters/MaterializedCTE.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Exception.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

struct MaterializedCteWithLevel
{
    QueryTreeNodePtr table_node;
    size_t level;
};

using CTEToLevelMap = std::unordered_map<MaterializedCTEPtr, MaterializedCteWithLevel>;

}

OrderedMaterializedCTEs collectMaterializedCTEs(const QueryTreeNodePtr & node, const SelectQueryOptions & select_query_options)
{
    if (select_query_options.is_subquery && !select_query_options.force_materialize_cte)
        return {};

    CTEToLevelMap materialized_ctes;
    OrderedMaterializedCTEs ctes_by_level;

    size_t level = 0;
    size_t max_level = 0;

    /// Nested levels from the first walk of a shared body. A later reference adds this offset.
    std::unordered_map<MaterializedCTEPtr, std::unordered_map<MaterializedCTEPtr, size_t>> relative_levels;
    std::unordered_set<const MaterializedCTE *> walked_bodies;
    struct WalkingBody
    {
        MaterializedCTEPtr cte;
        size_t entry_level;
    };
    std::vector<WalkingBody> walking;
    std::vector<char> entered_body;

    auto propagate = [&](const MaterializedCTEPtr & root_owner, size_t root_level)
    {
        std::vector<std::pair<MaterializedCTEPtr, size_t>> pending;
        pending.emplace_back(root_owner, root_level);

        /// A cycle of relative levels must not raise levels without end.
        const size_t cte_count = std::max<size_t>(materialized_ctes.size(), 1);
        const size_t hop_limit = cte_count * cte_count * cte_count;
        size_t hops = 0;

        while (!pending.empty() && hops < hop_limit)
        {
            ++hops;
            auto [owner, owner_level] = pending.back();
            pending.pop_back();

            auto owner_it = materialized_ctes.find(owner);
            if (owner_it == materialized_ctes.end() || owner_level < owner_it->second.level)
                continue;

            auto found = relative_levels.find(owner);
            if (found == relative_levels.end())
                continue;

            const size_t new_entry = owner_level + 1;
            for (const auto & [nested, relative] : found->second)
            {
                auto nested_it = materialized_ctes.find(nested);
                if (nested_it == materialized_ctes.end())
                    continue;

                const size_t candidate = new_entry + relative;
                if (candidate <= nested_it->second.level)
                    continue;

                nested_it->second.level = candidate;
                max_level = std::max(max_level, candidate);
                pending.emplace_back(nested, candidate);
            }
        }
    };

    auto qualifies = [&](const TableNode * table_node) -> MaterializedCTEPtr
    {
        if (!table_node)
            return {};

        const auto & cte = table_node->getMaterializedCTE();
        /// A subquery-less, non-plan-backed reference is the CTE's temp storage resolved by name
        /// (e.g. a per-shard local plan reading a shipped external table): nothing to materialize.
        /// A plan that may run as a standalone pipeline is the exception - nothing above it can
        /// gate its readers, so it plants its own gate.
        if (cte && (table_node->isMaterializedCTE() || cte->hasPlanOrBuilt()
                    || select_query_options.force_materialize_cte))
            return cte;

        return {};
    };

    traverseQueryTree(node, Everything{},
    [&](const QueryTreeNodePtr & current_node)
    {
        auto * table_node = current_node->as<TableNode>();
        auto cte = qualifies(table_node);
        if (!cte)
            return;

        const bool shares = table_node->sharesMaterializedCTEBody();
        const bool first_shared_walk = shares && walked_bodies.insert(cte.get()).second;

        auto [it, inserted] = materialized_ctes.emplace(cte, MaterializedCteWithLevel{current_node, level});
        if (!inserted && level > it->second.level)
        {
            it->second.level = level;
            if (shares && !first_shared_walk)
                propagate(cte, level);
        }

        max_level = std::max(max_level, it->second.level);

        for (const auto & frame : walking)
        {
            if (frame.cte == cte || level < frame.entry_level)
                continue;

            auto & slot = relative_levels[frame.cte][cte];
            slot = std::max(slot, level - frame.entry_level);
        }

        ++level;
        entered_body.push_back(first_shared_walk ? 1 : 0);
        if (first_shared_walk)
            walking.push_back(WalkingBody{cte, level});
    },
    [&](const QueryTreeNodePtr & current_node)
    {
        if (!qualifies(current_node->as<TableNode>()))
            return;

        const bool pushed = entered_body.back();
        entered_body.pop_back();
        if (pushed)
            walking.pop_back();
        --level;
    });

    if (materialized_ctes.empty())
        return ctes_by_level;

    ctes_by_level.resize(max_level + 1);
    for (const auto & [_, future_table] : materialized_ctes)
    {
        /// Deepest materialized CTEs should be executed first, because CTEs with lower levels depend on them.
        ctes_by_level[future_table.level].push_back(future_table.table_node);
    }

    return ctes_by_level;
}

void addBuildSubqueriesForMaterializedCTEsIfNeeded(
    QueryPlan & query_plan,
    const SelectQueryOptions & select_query_options,
    const OrderedMaterializedCTEs & materialized_ctes
)
{
    /// Logical plans are built for serialization to a remote node. `DelayedMaterializingCTEsStep`
    /// is stripped on the way out (`Serialization.cpp`), and the only surviving side effect of
    /// building it here would be to populate the shared `MaterializedCTE::plan` with a logical
    /// (serialize-only) version that the non-logical planner pass would then reuse for local
    /// execution and crash on. The materialization is owned by the non-logical pass; remote
    /// nodes read from the temp storage by name.
    if (select_query_options.build_logical_plan)
        return;

    if (materialized_ctes.empty())
        return;

    // The main idea of the algorithm is to unite plans for Materialized CTEs of the same level
    // with the main query plan by MaterializingCTEsStep.
    //
    // This allows to ensure following properties:
    // 1) All CTEs are executed before the main query.
    // 2) If CTE A depends on CTE B, then A will be executed after B, because A will be on the next level after B.
    // 3) CTEs on the same level are independent.
    // 3) CTEs of the same level will be executed in the same MaterializingCTEsStep, so they will be executed in parallel.
    // 4) Materialized CTEs are executed only once.
    //
    // Example of query plan structure for query with 2 levels of CTEs:
    //
    //                                  ┌───────────────────────┐
    //                                  │                       │
    //                             ┌────│ MaterializingCTEsStep │────────────────────────────┐
    //                             │    │                       │         │                  │
    //                             │    └───────────────────────┘         │                  │
    //                             │                                      │                  │
    //                             │                                      │                  │
    //                 ┌───────────▼───────────┐                 ┌────────▼───────┐ ┌────────▼───────┐
    //                 │                       │                 │                │ │                │
    //        ┌────────│ MaterializingCTEsStep │─────────┐       │ CTE (level: 0) │ │ CTE (level: 0) │
    //        │        │                       │         │       │                │ │                │
    //        │        └───────────────────────┘         │       └────────────────┘ └────────────────┘
    //        │                                          │
    //        │                                          │
    // ┌──────▼─────┐                           ┌────────▼───────┐
    // │            │                           │                │
    // │ Query Plan │                           │ CTE (level: 1) │
    // │            │                           │                │
    // └────────────┘                           └────────────────┘
    //
    // The CTEs are added as DelayedMaterializingCTEsStep nodes — one per level — so that
    // resolveMaterializingCTEs can skip already-materialized CTEs. This is important when
    // buildOrderedSetInplace runs a subquery plan that contains CTEs: by the time the main
    // plan's resolveMaterializingCTEs fires, is_planned is already true for those CTEs
    // so they won't be materialized a second time.
    //
    // The level structure is preserved: for each level we push one DelayedMaterializingCTEsStep
    // on top of the current plan, wrapping it the same way the old eager approach did with
    // MaterializingCTEsStep. resolveMaterializingCTEs processes nodes post-order, so the inner
    // (lower-level) step is resolved before the outer one, guaranteeing that a CTE at level N
    // is always materialized before the CTE at level N-1 that depends on it.
    for (const auto & cte_level : materialized_ctes)
    {
        std::vector<MaterializedCTEPtr> ctes;
        ctes.reserve(cte_level.size());

        for (const auto & cte_node : cte_level)
        {
            auto * cte_table_node = cte_node->as<TableNode>();
            auto materialized_cte = cte_table_node->getMaterializedCTE();
            if (!materialized_cte->hasPlanOrBuilt())
            {
                auto cte_subquery = cte_table_node->getMaterializedCTESubquery();
                /// A by-name reference carries no subquery, but a standalone pipeline still needs a
                /// gate for it, and the handle alone is enough to build one. The writer stays with
                /// whoever holds the subquery.
                if (!cte_subquery && select_query_options.force_materialize_cte)
                {
                    ctes.push_back(materialized_cte);
                    continue;
                }
                if (!cte_subquery)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "CTE '{}' does not have query tree, but was not planned yet",
                        materialized_cte->cte_name);

                auto cte_options = select_query_options.subquery();
                /// Gates for a materialized CTE belong to the plan that collected it, where a deeper
                /// level yields a higher and therefore dominating gate; this CTE's plan is not that plan.
                cte_options.forceMaterializeCTE(false);
                Planner cte_planner(
                    cte_subquery,
                    cte_options,
                    std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}));
                cte_planner.buildQueryPlanIfNeeded();

                auto cte_plan = std::move(cte_planner).extractQueryPlan();
                /// The CTE plan is kept aside until `optimize`, after the distributed-plan decision, so its
                /// contexts have to follow this plan's decision from here.
                query_plan.takeContextsFrom(cte_plan);

                auto step = std::make_unique<MaterializingCTEStep>(
                    cte_plan.getCurrentHeader(),
                    materialized_cte);
                step->setStepDescription("Materializing CTE: " + materialized_cte->cte_name, 100);
                cte_plan.addStep(std::move(step));
                materialized_cte->plan = std::make_unique<QueryPlan>(std::move(cte_plan));
            }

            ctes.push_back(materialized_cte);
        }

        auto delayed_step = std::make_unique<DelayedMaterializingCTEsStep>(
            query_plan.getCurrentHeader(),
            std::move(ctes));
        query_plan.addStep(std::move(delayed_step));
    }
}

}
