#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/FilterStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromLocalParallelReplicaStep::ReadFromLocalParallelReplicaStep(
    QueryPlanPtr query_plan_, ContextPtr subquery_context_, bool replicas_get_pushed_conditions_)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
    , context(std::move(subquery_context_))
    , replicas_get_pushed_conditions(replicas_get_pushed_conditions_)
{
}

void ReadFromLocalParallelReplicaStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called", __PRETTY_FUNCTION__);
}

QueryPlanPtr ReadFromLocalParallelReplicaStep::extractQueryPlan()
{
    chassert(query_plan);

    auto qp = std::move(query_plan);
    query_plan.reset();
    return qp;
}

void ReadFromLocalParallelReplicaStep::restrictFixedColumnsToOwnFilters()
{
    if (!query_plan || !query_plan->isInitialized())
        return;

    /// One set per coordinated read - a fragment can hold several (see `findReadingSteps`) - each taken
    /// on the chain `buildSortingDAG` walks to reach that read: first child at every step. The walk is
    /// not reproduced here, `collectFixedColumnNames` runs that same function; what this loop writes out
    /// is only where each chain starts. Test 05161 holds it to that: two reads under a `UNION ALL`,
    /// naming the same column, one of which must not be given what the other's branch fixed.
    struct Frame
    {
        QueryPlan::Node * node;
        QueryPlan::Node * chain_root;
    };

    std::vector<Frame> stack{{query_plan->getRootNode(), query_plan->getRootNode()}};
    while (!stack.empty())
    {
        auto [node, chain_root] = stack.back();
        stack.pop_back();

        /// Only a read that announces to the coordinator has a mode to agree on. A read the fragment
        /// performs entirely on this node - a small joined table, say - is nobody's business but this
        /// replica's, and restricting it would cost an ordering for nothing.
        ///
        /// This set is the answer for a fragment whose conditions do not reach the replicas: they fix
        /// what it fixes on its own, and nothing else may order its read. Where they do reach them the
        /// read may order itself by everything pushed in as well, and `tryPushDownFilter` then leaves
        /// the set unset rather than widening it.
        ///
        /// Hence once per read, and only the first time: every later condition arrives with the earlier
        /// ones already spliced into this plan as `FilterStep`s, and a set taken then would count those
        /// among what the fragment fixes on its own.
        auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get());
        if (reading && reading->isParallelReadingFromReplicas() && !reading->getFixedColumnRestriction().has_value())
            reading->restrictFixedColumns(QueryPlanOptimizations::collectFixedColumnNames(*chain_root));

        for (size_t i = 0; i < node->children.size(); ++i)
            stack.push_back({node->children[i], i == 0 ? chain_root : node->children[i]});
    }
}

void ReadFromLocalParallelReplicaStep::addFilter(FilterDAGInfo filter)
{
    output_header = std::make_shared<const Block>(
        FilterTransform::transformHeader(*output_header, &filter.actions, filter.column_name, filter.do_remove_column));

    auto filter_step = std::make_unique<FilterStep>(
        query_plan->getCurrentHeader(), std::move(filter.actions), std::move(filter.column_name), filter.do_remove_column);
    query_plan->addStep(std::move(filter_step));
}

}
