#include <Interpreters/universalizePlan.h>

#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Analyzer/TableExpressionModifiers.h>

#include <stack>

namespace DB
{

/// Walks the plan tree checking whether every step can be
/// serialized for the query plan cache.
static bool isSerializablePlanImpl(const QueryPlan & plan)
{
    if (!plan.isInitialized())
        return false;

    std::stack<const QueryPlan::Node *> stack;
    stack.push(plan.getRootNode());

    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();

        for (const auto * child : node->children)
            stack.push(child);

        /// `DelayedCreatingSetsStep` wraps IN (...) sub-plans.
        /// Recursively validate each sub-plan.
        if (const auto * delayed = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
        {
            for (const auto & set : delayed->getSets())
            {
                if (const auto * sub_plan = set->getQueryPlan())
                    if (!isSerializablePlanImpl(*sub_plan))
                        return false;
            }
            continue;
        }

        const bool is_leaf = node->children.empty();

        if (is_leaf)
        {
            /// Leaf: `ReadFromMergeTree` is replaceable; others
            /// must declare `isSerializable`.
            /// TODO: Views expand into sub-plans whose leaf nodes are not `ReadFromMergeTree`
            /// (e.g. `ReadFromSubquery`). To support Views we need to recognize and recurse
            /// into these expanded sub-plans rather than rejecting them here.
            if (typeid_cast<const ReadFromMergeTree *>(node->step.get()))
                continue;
            if (node->step->isSerializable())
                continue;
            return false;
        }
        else
        {
            /// Non-leaf nodes must declare `isSerializable`.
            if (!node->step->isSerializable())
                return false;
        }
    }

    return true;
}

bool isSerializablePlan(const QueryPlan & plan)
{
    return isSerializablePlanImpl(plan);
}

/// Replaces `ReadFromMergeTree` leaves with storage-agnostic
/// `ReadFromTableStep` so the plan is safe to cache and reuse.
static void universalizePlanImpl(QueryPlan & plan)
{
    if (!plan.isInitialized())
        return;

    /// Guard: universalizePlan must be called on a pre-optimization plan.
    /// LazilyReadFromMergeTree nodes are created by the query plan optimizer; their presence
    /// indicates that optimization has already run, after which universalizePlan is unsafe.
    /// The check is O(n) in plan size — acceptable since it only runs on cache miss.
#ifndef NDEBUG
    {
        std::stack<const QueryPlan::Node *> check_stack;
        check_stack.push(plan.getRootNode());
        while (!check_stack.empty())
        {
            const auto * n = check_stack.top();
            check_stack.pop();
            for (const auto * c : n->children)
                check_stack.push(c);
            chassert(
                typeid_cast<const LazilyReadFromMergeTree *>(n->step.get()) == nullptr
                && "universalizePlan called on a post-optimization plan (LazilyReadFromMergeTree detected)");
        }
    }
#endif

    std::stack<QueryPlan::Node *> stack;
    stack.push(plan.getRootNode());

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        for (auto * child : node->children)
            stack.push(child);

        /// Recursively universalize sub-plans inside IN (...)
        /// subqueries.
        if (const auto * delayed = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
        {
            for (const auto & set : delayed->getSets())
                if (auto * sub_plan = set->getQueryPlan())
                    universalizePlanImpl(*sub_plan);
            continue;
        }

        /// Only process leaf nodes.
        if (!node->children.empty())
            continue;

        /// Replace `ReadFromMergeTree` with `ReadFromTableStep`.
        /// TODO: When adding View support, this block must also handle leaf nodes produced by
        /// View expansion (e.g. sub-plans that read from the View's underlying tables) and
        /// universalize each underlying `ReadFromMergeTree` accordingly.
        auto * read_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
        if (!read_step)
            continue;

        const auto & query_info = read_step->getQueryInfo();
        const auto table_name = read_step->getStorageID().getFullTableName();
        const auto & output_header = read_step->getOutputHeader();
        const auto modifiers = query_info.table_expression_modifiers.value_or(TableExpressionModifiers{});
        const bool use_parallel_replicas = read_step->isParallelReadingFromReplicas();
        const auto prewhere_info = query_info.prewhere_info;
        const auto row_level_filter = query_info.row_level_filter;

        /// `ReadFromTableStep` stores table_name + modifiers
        /// (FINAL, SAMPLE) + parallel-replicas flag + optional
        /// explicit PREWHERE info.
        ///
        /// `resolveStorages` reads these back and calls
        /// `storage->read` with `QueryProcessingStage::FetchColumns`
        /// to create a fresh `ReadFromMergeTree` against the current
        /// data snapshot.
        ///
        /// PREWHERE handling:
        ///
        /// 1. Automatic PREWHERE (`optimize_move_to_prewhere = true`, the default):
        ///    The optimizer's `FilterStep` remains in the cached plan. On cache hit,
        ///    `QueryPlan::optimize` re-runs and `MergeTreeWhereOptimizer` re-derives
        ///    PREWHERE from the `FilterStep` via `applyFilters`. This path is safe.
        ///
        /// 2. Explicit `PREWHERE` clause in SQL:
        ///    The Planner embeds the PREWHERE expression directly into
        ///    `ReadFromMergeTree.prewhere_info`. We preserve it in `ReadFromTableStep`
        ///    so that `resolveStorages` can restore it onto `SelectQueryInfo` before
        ///    calling `storage->read`, keeping the I/O reduction benefit.
        ///
        /// Row-level security (row policy): for storages that support PREWHERE the Planner
        /// puts the row policy into `SelectQueryInfo::row_level_filter` (not a `FilterStep`),
        /// so `ReadFromMergeTree` carries it via `query_info`. We must preserve it the same
        /// way as PREWHERE; otherwise `resolveStorages` would rebuild the read without the
        /// policy on a cache hit and return unfiltered rows.
        node->step = std::make_unique<ReadFromTableStep>(
            output_header, table_name, modifiers, use_parallel_replicas, prewhere_info, row_level_filter);
    }
}

void universalizePlan(QueryPlan & plan)
{
    universalizePlanImpl(plan);
}

}

