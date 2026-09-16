#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Interpreters/IJoin.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{
namespace QueryPlanOptimizations
{

namespace
{

/// Steps a join and the reading steps of its inputs may be separated by.
bool isTransparentForSealGating(IQueryPlanStep * step)
{
    return typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step);
}

/// The reading step of the join's probe input, if the input is a plain read (possibly
/// behind expressions and filters). The applied runtime filters are already registered on
/// it by collectAppliedJoinRuntimeFilters, with the key names resolved to physical columns.
ReadFromMergeTree * findProbeSideReading(QueryPlan::Node * node)
{
    while (node)
    {
        if (auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get()))
            return reading;

        if (!isTransparentForSealGating(node->step.get()) || node->children.size() != 1)
            return nullptr;

        node = node->children.front();
    }
    return nullptr;
}

/// Find the build-side runtime filter step with the given rendezvous key. A multi-key join
/// plants a chain of BuildRuntimeFilterSteps, one per key: walk through the whole chain.
BuildRuntimeFilterStep * findBuildSideRuntimeFilter(QueryPlan::Node * node, const String & filter_key)
{
    while (node)
    {
        IQueryPlanStep * step = node->step.get();

        if (auto * build_step = typeid_cast<BuildRuntimeFilterStep *>(step))
        {
            if (build_step->getFilterKey() == filter_key)
                return build_step;
        }
        else if (!isTransparentForSealGating(step))
        {
            return nullptr;
        }

        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    return nullptr;
}

void tryMarkJoin(QueryPlan::Node & node)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return;

    /// Only the hash-join pipeline (build the right side first) wires the seal.
    if (join_step->getJoin()->pipelineType() != JoinPipelineType::FillRightFirst)
        return;

    const size_t probe_idx = join_step->swap_streams ? 1 : 0;

    auto * reading = findProbeSideReading(node.children[probe_idx]);
    if (!reading)
        return;

    /// The applied runtime filters of the read, registered by collectAppliedJoinRuntimeFilters
    /// (they may belong to several joins of the plan; the build-side lookup below associates
    /// each with this join or rejects it).
    const auto & matches = reading->getJoinRuntimeFiltersForIndexAnalysis();
    if (matches.empty())
        return;

    if (reading->isQueryWithFinal() || reading->isParallelReadingEnabled())
        return;

    /// Cover the longest primary-key PREFIX with the matched filters, in the key order: only
    /// a prefix condition can cut mark ranges (a filter on a non-leading key column selects
    /// rows scattered over the whole part), so gating on anything else would only delay the
    /// probe side. Each covering filter must be built by THIS join's build side and be
    /// guaranteed to convert into a positive predicate once complete: a NOT-contains (ANTI)
    /// filter never does, and an exact-set-only key type may lose its set to a bloom-filter
    /// overflow.
    const auto & primary_key_columns = reading->getStorageMetadata()->getPrimaryKey().column_names;
    std::vector<RuntimeFilterIndexAnalysisDescriptor> prefix;
    std::vector<BuildRuntimeFilterStep *> build_steps;
    for (const auto & pk_column : primary_key_columns)
    {
        const RuntimeFilterIndexAnalysisDescriptor * covering = nullptr;
        BuildRuntimeFilterStep * covering_build_step = nullptr;
        for (const auto & descr : matches)
        {
            if (descr.key_column_name != pk_column)
                continue;
            if (auto * build_step = findBuildSideRuntimeFilter(node.children[1 - probe_idx], descr.filter_id))
            {
                covering = &descr;
                covering_build_step = build_step;
                break;
            }
        }

        if (!covering || !covering_build_step->canSealPrunePrimaryKey())
            break;

        prefix.push_back(*covering);
        build_steps.push_back(covering_build_step);
    }

    if (prefix.empty())
        return;

    /// The filters must record the exact key values (or at least the range envelope) for the
    /// seal to prune anything; this is off by default (it costs a bit at build time) unless
    /// enable_join_runtime_filters_index_analysis already turned it on.
    for (auto * build_step : build_steps)
        build_step->enableKeyRangeTracking();

    /// The gating (pipeline) edge is keyed by the leading filter; the refiner picks up the
    /// whole prefix from the runtime filter lookup once the seal arrives (all the filters of
    /// this join are complete by then: their transforms are upstream of the seal emitter).
    join_step->enableSealGatedProbeReading(prefix.front().filter_id);
    reading->enableSealGatedReading(std::move(prefix));
}

}

void markSealGatedReading(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack = {&root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        tryMarkJoin(*node);

        for (auto * child : node->children)
            stack.push_back(child);
    }
}

}
}
