#include <Planner/CollectMaterializedCTE.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/traverseQueryTree.h>
#include <Interpreters/MaterializedCTE.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Exception.h>

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
    QueryTreeNodePtr subquery;
    size_t level;
};

using CTEToLevelMap = std::unordered_map<MaterializedCTEPtr, MaterializedCteWithLevel>;

/// Registers one occurrence of `cte`. The deepest level wins, because that is the level whose gate
/// dominates every shallower reader; a null `subquery` is upgraded by any occurrence carrying a body,
/// which is what a writer can be built from. Returns true iff the entry is new or its level rose.
bool registerMaterializedCTE(
    CTEToLevelMap & materialized_ctes,
    const MaterializedCTEPtr & cte,
    const QueryTreeNodePtr & subquery,
    size_t level)
{
    auto [it, inserted] = materialized_ctes.emplace(cte, MaterializedCteWithLevel{subquery, level});
    if (inserted)
        return true;

    if (!it->second.subquery && subquery)
        it->second.subquery = subquery;

    if (it->second.level >= level)
        return false;

    it->second.level = level;
    return true;
}

}

OrderedMaterializedCTEs collectMaterializedCTEs(const QueryTreeNodePtr & node, const SelectQueryOptions & select_query_options)
{
    if (select_query_options.is_subquery && !select_query_options.force_materialize_cte)
        return {};

    CTEToLevelMap materialized_ctes;
    OrderedMaterializedCTEs ctes_by_level;

    size_t level = 0;
    traverseQueryTree(node, Everything{},
    [&](const QueryTreeNodePtr & current_node)
    {
        if (auto * table_node = current_node->as<TableNode>())
        {
            const auto & cte = table_node->getMaterializedCTE();
            /// A subquery-less, non-plan-backed reference is the CTE's temp storage resolved by name
            /// (e.g. a per-shard local plan reading a shipped external table): nothing to materialize.
            /// A plan that may run as a standalone pipeline is the exception - nothing above it can
            /// gate its readers, so it plants its own gate.
            if (cte && (table_node->isMaterializedCTE() || cte->hasPlanOrBuilt()
                        || select_query_options.force_materialize_cte))
            {
                registerMaterializedCTE(materialized_ctes, cte, table_node->getMaterializedCTESubquery(), level);
                ++level;
            }
        }
    },
    [&level, &select_query_options](const QueryTreeNodePtr & current_node)
    {
        if (auto * table_node = current_node->as<TableNode>())
        {
            const auto & cte = table_node->getMaterializedCTE();
            if (cte && (table_node->isMaterializedCTE() || cte->hasPlanOrBuilt()
                        || select_query_options.force_materialize_cte))
                --level;
        }
    });

    if (materialized_ctes.empty())
        return ctes_by_level;

    /// A by-name reference carries no body, so the CTEs that body reads are invisible to the walk
    /// above. They belong one level deeper than the CTE reading them, so that their gate dominates.
    for (size_t iteration = 0;; ++iteration)
    {
        std::vector<std::pair<MaterializedCTEPtr, size_t>> known;
        known.reserve(materialized_ctes.size());
        for (const auto & [cte, entry] : materialized_ctes)
            known.emplace_back(cte, entry.level);

        bool level_changed = false;
        for (const auto & [cte, cte_level] : known)
            for (const auto & dependency : cte->dependencies)
                level_changed |= registerMaterializedCTE(materialized_ctes, dependency, nullptr, cte_level + 1);

        if (!level_changed)
            break;

        /// A level can only keep rising past one round per CTE if the dependencies contain a cycle,
        /// which no ordering of writers satisfies.
        if (iteration >= materialized_ctes.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Dependencies of materialized CTEs form a cycle, their materialization cannot be ordered");
    }

    size_t max_level = 0;
    for (const auto & [_, entry] : materialized_ctes)
        max_level = std::max(max_level, entry.level);

    ctes_by_level.resize(max_level + 1);
    for (const auto & [cte, entry] : materialized_ctes)
    {
        /// Deepest materialized CTEs should be executed first, because CTEs with lower levels depend on them.
        ctes_by_level[entry.level].push_back(CollectedMaterializedCTE{cte, entry.subquery});
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

        for (const auto & collected_cte : cte_level)
        {
            const auto & materialized_cte = collected_cte.cte;
            if (!materialized_cte->hasPlanOrBuilt())
            {
                const auto & cte_subquery = collected_cte.subquery;
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
