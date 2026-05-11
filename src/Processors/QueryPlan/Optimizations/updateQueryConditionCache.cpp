#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Functions/IFunction.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/functional/hash.hpp>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Like `VirtualColumnUtils::isDeterministic`, but treats `__topKFilter` as deterministic.
///
/// `__topKFilter` is the only "officially" non-deterministic function we expect to appear in
/// `query_info.filter_actions_dag` once `tryOptimizeTopK` has chosen the read step for TopK
/// dynamic filtering: filter pushdown collects the PREWHERE `__topKFilter` together with the
/// WHERE predicate. The non-determinism is bounded — for a fixed plan and data, the threshold
/// trajectory only tightens, so any row whose sort-column value lies in the final top-N is
/// kept by `__topKFilter` at every point during execution. Consequently a chunk that the
/// outer `WHERE` reduces to zero rows is one whose granule has no row that could have
/// reached the final result, regardless of the threshold's exact path through the run. The
/// cache key is also salted with the TopK plan parameters, so cached granule decisions can
/// only be reused under the same TopK plan that produced them.
bool isDeterministicAllowingTopKFilter(const ActionsDAG::Node * node)
{
    for (const auto * child : node->children)
        if (!isDeterministicAllowingTopKFilter(child))
            return false;

    if (node->type == ActionsDAG::ActionType::COLUMN)
        return node->isDeterministic();

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    if (!node->function_base->isDeterministic())
        return node->function_base->getName() == "__topKFilter";

    return true;
}

}

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

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto & filter_actions_dag = query_info.filter_actions_dag;
    if (!filter_actions_dag || query_info.isFinal())
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

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
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
            filter_step->setConditionForQueryConditionCache(condition_hash, condition);
            return;
        }
    }
}

}
