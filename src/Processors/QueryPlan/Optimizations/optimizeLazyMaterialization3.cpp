#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Whether a second, row-addressed read of this source is possible, which is what deferring a column of
/// it comes down to. The same conditions the other lazy materialization path applies to its one source.
bool canReadLazily(const QueryPlan::Node & source, const QueryPlanOptimizationSettings & settings)
{
    auto * step = source.step.get();

    if (auto * merge_tree = typeid_cast<ReadFromMergeTree *>(step))
    {
        /// Allow FINAL only for ReplacingMergeTree.
        if (merge_tree->isQueryWithFinal()
            && merge_tree->getMergeTreeData().merging_params.mode != MergeTreeData::MergingParams::Replacing)
            return false;

        if (merge_tree->isQueryWithSampling())
            return false;

        return !merge_tree->getMutationsSnapshot()->hasPatchParts();
    }

    if (auto * object_storage = typeid_cast<ReadFromObjectStorageStep *>(step))
        return settings.optimize_lazy_materialization_for_object_storage && object_storage->canUseLazyMaterialization();

    return false;
}

}

bool optimizeLazyMaterialization3(
    QueryPlan::Node & root, QueryPlan & /*query_plan*/, QueryPlan::Nodes & /*nodes*/,
    const QueryPlanOptimizationSettings & settings, size_t max_limit_for_lazy_materialization)
{
    if (root.children.size() != 1)
        return false;

    auto * limit_step = typeid_cast<LimitStep *>(root.step.get());
    if (!limit_step)
        return false;

    /// It is not known how many rows LIMIT WITH TIES reads, so there is no telling what a second read
    /// would have to fetch.
    if (limit_step->withTies())
        return false;

    const auto limit = limit_step->getLimit();
    if (limit == 0 || (max_limit_for_lazy_materialization != 0 && limit > max_limit_for_lazy_materialization))
        return false;

    /// The chain of steps down to the sources starts below the sorting, or below the limit when the
    /// query has no ORDER BY.
    auto * chain_top = root.children.front();
    SortDescription sort_description;
    if (auto * sorting_step = typeid_cast<SortingStep *>(chain_top->step.get()))
    {
        if (sorting_step->getType() != SortingStep::Type::Full && sorting_step->getType() != SortingStep::Type::FinishSorting)
            return false;

        sort_description = sorting_step->getSortDescription();
        chain_top = chain_top->children.front();
    }

    const auto merged = buildMergedPlanDAG(*chain_top);

    std::vector<bool> lazy_sources(merged.sources.size(), false);
    bool has_lazy_source = false;
    for (size_t source = 0; source < merged.sources.size(); ++source)
    {
        lazy_sources[source] = canReadLazily(*merged.sources[source].plan_node, settings);
        has_lazy_source |= lazy_sources[source];
    }

    if (!has_lazy_source)
        return false;

    /// The sort keys are needed below the LIMIT whatever else is deferred.
    const auto & outputs = merged.getOutputs();
    std::vector<size_t> eager_output_positions;
    for (const auto & description : sort_description)
    {
        for (size_t position = 0; position < outputs.size(); ++position)
        {
            if (outputs[position]->result_name == description.column_name)
            {
                eager_output_positions.push_back(position);
                break;
            }
        }
    }
    std::ranges::sort(eager_output_positions);

    const auto frontier = chooseLazyFrontier(merged, eager_output_positions, lazy_sources);
    if (!frontier.defersAnything())
        return false;

    /// What is left is to rebuild the plan around that decision, which needs one thing the merged DAG
    /// does not record yet: which step of the original chain each of its values came from.
    ///
    /// The main branch cannot be one expression over the whole subtree, because the steps it came from
    /// are not interchangeable. A filter decides which rows the steps above it ever see, so computing
    /// something above it below it instead can throw where the query does not - `where x != 0` followed
    /// by `intDiv(1, x)` is the short case. The rebuild therefore has to walk the same steps in the same
    /// order and take from each one the values this decided to compute below the `LIMIT`, which means
    /// mapping each value back to the step that computes it.
    LOG_TRACE(
        getLogger("QueryPlanOptimizeLazyMaterialization"),
        "Merged DAG: {} sources, {} values; frontier defers something, but the rebuild is not implemented yet",
        merged.sources.size(), merged.getDAG().getNodes().size());

    return false;
}

}
