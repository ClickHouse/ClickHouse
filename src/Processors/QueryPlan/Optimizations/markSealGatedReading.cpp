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

/// The first `__applyFilter` conjunct found in the DAG, if any (see findAppliedRuntimeFilters).
std::optional<RuntimeFilterIndexAnalysisDescriptor> findAppliedRuntimeFilter(const ActionsDAG & dag)
{
    auto descriptors = findAppliedRuntimeFilters(dag);
    if (descriptors.empty())
        return {};
    return std::move(descriptors.front());
}

struct ProbeSide
{
    ReadFromMergeTree * reading = nullptr;
    /// The first `__applyFilter` conjunct found on the way.
    std::optional<RuntimeFilterIndexAnalysisDescriptor> runtime_filter;
};

/// The runtime filter is planted as a FilterStep conjunct above the reading step (it is not
/// pushed into the reading step's own filter, which is populated earlier in the second pass),
/// so look for it while walking down. The reading step's own filter and prewhere are checked
/// too in case a later optimization moved the conjunct there.
ProbeSide findProbeSide(QueryPlan::Node * node)
{
    ProbeSide res;
    while (node)
    {
        if (!res.runtime_filter)
        {
            if (const auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
                res.runtime_filter = findAppliedRuntimeFilter(filter_step->getExpression());
        }

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        {
            res.reading = reading;
            if (!res.runtime_filter)
            {
                if (const auto & dag = reading->getFilterActionsDAG())
                    res.runtime_filter = findAppliedRuntimeFilter(*dag);
                if (!res.runtime_filter && reading->getQueryInfo().prewhere_info)
                    res.runtime_filter = findAppliedRuntimeFilter(reading->getQueryInfo().prewhere_info->prewhere_actions);
            }
            return res;
        }

        if (!isTransparentForSealGating(node->step.get()) || node->children.size() != 1)
            return res;

        node = node->children.front();
    }
    return res;
}

/// Find the build-side runtime filter step with the given rendezvous key.
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

    auto [reading, match] = findProbeSide(node.children[probe_idx]);
    if (!reading || !match)
        return;

    if (reading->isQueryWithFinal() || reading->isParallelReadingEnabled())
        return;

    auto * build_step = findBuildSideRuntimeFilter(node.children[1 - probe_idx], match->filter_id);
    if (!build_step)
        return;

    /// Gating pays off only if the completed filter converts into a positive primary-key
    /// predicate: a NOT-contains (ANTI) filter never does, and an exact-set-only key type
    /// may lose its set to a bloom-filter overflow. Such probes are better read ungated,
    /// overlapping with the build, with row-level filtering only.
    if (!build_step->canSealPrunePrimaryKey())
        return;

    /// The filter can prune ranges only through the primary key.
    const auto & primary_key_columns = reading->getStorageMetadata()->getPrimaryKey().column_names;
    if (std::find(primary_key_columns.begin(), primary_key_columns.end(), match->key_column_name) == primary_key_columns.end())
        return;

    /// The filter must record the exact key values (or at least the range envelope) for the
    /// seal payload to prune anything; this is off by default (it costs a bit at build time)
    /// unless enable_join_runtime_filters_index_analysis already turned it on.
    build_step->enableKeyRangeTracking();

    reading->enableSealGatedReading(match->key_column_name, match->key_column_type, match->filter_id);
    join_step->enableSealGatedProbeReading(match->filter_id);
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
