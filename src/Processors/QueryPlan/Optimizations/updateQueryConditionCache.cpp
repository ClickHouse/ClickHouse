#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Functions/IFunction.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/functional/hash.hpp>

namespace DB::QueryPlanOptimizations
{

using VirtualColumnUtils::isDeterministicAllowingTopKFilter;

/// This is not really an optimization. The purpose of this function is to extract and hash the filter condition of WHERE or PREWHERE
/// filters. These correspond to these steps:
///
///   [...]
///     ^
///     |
///     |
///   FilterStep
///     ^
///     |
///     |
///   ReadFromMergeTree
///
/// Later on, the hashed filter condition will be used as a key in the query condition cache.
///
void updateQueryConditionCache(const Stack & stack, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.use_query_condition_cache)
        return;

    const auto & frame = stack.back();

    auto * read_from_merge_tree = dynamic_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree)
        return;

    /// The query condition cache for `ORDER BY ... LIMIT N` (TopK) reads is gated behind the
    /// `use_query_condition_cache_for_top_k` setting (enabled by default). When it is off, skip the
    /// QCC write for the WHERE filter of a TopK read: such reads can drop granules during execution
    /// depending on the running `__topKFilter` threshold, so the WHERE filter's "matches no rows"
    /// result no longer holds for every granule of the part.
    if (!optimization_settings.use_query_condition_cache_for_top_k
        && read_from_merge_tree->isSelectedForTopKFilterOptimization())
        return;

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto & filter_actions_dag = query_info.filter_actions_dag;
    if (!filter_actions_dag || query_info.isFinal())
        return;

    /// The read step neither consults nor populates the cache for such filters (see the comment at
    /// the declaration), so don't tag the filter step either.
    if (ReadFromMergeTree::filterDependsOnNonDeterministicVirtuals(read_from_merge_tree->getStorageMetadata()->virtuals, query_info))
        return;

    const auto & outputs = filter_actions_dag->getOutputs();

    /// Restrict to the case that ActionsDAG has a single output. This isn't technically necessary but de-risks
    /// the implementation a lot while not losing much usefulness.
    if (outputs.size() != 1)
        return;

    /// Issues #81506 and #84508.
    for (const auto * output : outputs)
    {
        if (!isDeterministicAllowingTopKFilter(output))
            return;
    }

    const bool is_top_k_read = read_from_merge_tree->isSelectedForTopKFilterOptimization();

    FilterStep * filter_step_to_tag = nullptr;
    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get());
        if (!filter_step)
            continue;

        const auto * filter_node = filter_step->getExpression().tryFindInOutputs(filter_step->getFilterColumnName());
        if (!filter_node || !isDeterministicAllowingTopKFilter(filter_node))
        {
            /// Only tag the storage WHERE filter, not one carrying e.g. `__applyFilter`.
            /// For a TopK read this also covers a non-deterministic filter *higher* in the stack
            /// (e.g. a join runtime filter that stayed a separate step): it changes the running
            /// threshold, so the WHERE key below it is unsound too — the same reason
            /// `disableTopKQueryConditionCacheUnderNonDeterministicFilters` drops such keys.
            /// This walk may run again after that pass (the post-lazy-materialization re-tagging
            /// in `optimizeTree`), so it must not recreate the key here.
            return;
        }

        if (!filter_step_to_tag)
        {
            filter_step_to_tag = filter_step;
            /// A filter above the WHERE filter cannot invalidate the key of a regular read
            /// (granule elision by WHERE does not depend on what happens above), so only a
            /// TopK read needs the full-stack scan for non-deterministic filters.
            if (!is_top_k_read)
                break;
        }
    }

    if (!filter_step_to_tag)
        return;

    /// `size_t` (not `UInt64`) so `boost::hash_combine` binds on platforms where
    /// they differ (e.g. Apple, where `size_t` is `unsigned long` but `UInt64` is `unsigned long long`).
    size_t condition_hash = filter_actions_dag->getOutputs()[0]->getHash();

    /// `ORDER BY ... LIMIT N` may drop granules during reading, so the result of the WHERE
    /// filter is no longer "applies to every granule of every part" — it applies only to
    /// the granules that the TopK filter decided to keep. To keep the QCC entry sound, we
    /// fold the deterministic part of the TopK plan into the cache key. Same query + same
    /// part set + same TopK params → cache hit; different LIMIT or sort column → fresh
    /// entry, never reusing a row-set computed under different TopK conditions.
    if (const auto & top_k_filter_info = read_from_merge_tree->getTopKFilterInfo())
        boost::hash_combine(condition_hash, top_k_filter_info->condition_hash);

    String condition = filter_actions_dag->getNames()[0];
    filter_step_to_tag->setConditionForQueryConditionCache(condition_hash, condition);
}


/// Join runtime filters (`__applyFilter`) are injected and pushed down after the main
/// `updateQueryConditionCache` walk, so the walk above cannot see them. They change which rows
/// reach the sorter and therefore the running TopK threshold, but their per-execution contents are
/// not part of the query condition cache key. Run this pass after runtime filters are in place: for
/// a TopK read with such a filter above it, disable the TopK PREWHERE cache (reuse and write) and
/// drop the TopK-salted WHERE key that the earlier walk may have attached.
/// `__topKFilter` itself is deliberately allowed: its plan salt is part of those keys.
void disableTopKQueryConditionCacheUnderNonDeterministicFilters(const Stack & stack, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.use_query_condition_cache)
        return;

    const auto & frame = stack.back();

    auto * read_from_merge_tree = dynamic_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree || !read_from_merge_tree->isSelectedForTopKFilterOptimization())
        return;

    /// The `FilterStep` directly above the read is the one that `updateQueryConditionCache` tags
    /// with the WHERE cache key; any non-deterministic filter anywhere above the read invalidates
    /// both that key and the PREWHERE entries.
    FilterStep * tagged_filter_step = nullptr;
    bool has_non_deterministic_filter = false;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get());
        if (!filter_step)
            continue;

        const auto * filter_node = filter_step->getExpression().tryFindInOutputs(filter_step->getFilterColumnName());
        if (!filter_node || !isDeterministicAllowingTopKFilter(filter_node))
        {
            has_non_deterministic_filter = true;
            break;
        }

        if (!tagged_filter_step)
            tagged_filter_step = filter_step;
    }

    if (!has_non_deterministic_filter)
        return;

    read_from_merge_tree->disableTopKPrewhereQueryConditionCache();
    if (tagged_filter_step)
        tagged_filter_step->resetConditionForQueryConditionCache();
}

}
