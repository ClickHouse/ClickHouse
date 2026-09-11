#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/UnionStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromLocalParallelReplicaStep::ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_, ContextPtr subquery_context_)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
    , context(std::move(subquery_context_))
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

    /// A read derives ordering only from the filters read-in-order can reach above it, and it reaches
    /// them by taking the first child at every step - through a join or a `UNION ALL` view alike. So a
    /// read is held to the filters on the longest such chain ending in it, and each read gets its own
    /// set: a fragment can hold several coordinated reads (see `findReadingSteps`), and one set taken at
    /// the root would describe only the branch the first children lead to. Handing that set to a read in
    /// another branch would allow a column its own branch never fixed, whenever the two branches happen
    /// to name a column alike.
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
        if (auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get()); reading && reading->isParallelReadingFromReplicas())
            reading->restrictFixedColumns(QueryPlanOptimizations::collectFixedColumnNames(*chain_root));

        for (size_t i = 0; i < node->children.size(); ++i)
            stack.push_back({node->children[i], i == 0 ? chain_root : node->children[i]});
    }
}

bool ReadFromLocalParallelReplicaStep::remoteRewriteRefusesThisShape() const
{
    if (!query_plan || !query_plan->isInitialized())
        return false;

    std::vector<const QueryPlan::Node *> stack{query_plan->getRootNode()};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (typeid_cast<const JoinStep *>(node->step.get()) || typeid_cast<const JoinStepLogical *>(node->step.get())
            || typeid_cast<const FilledJoinStep *>(node->step.get()) || typeid_cast<const UnionStep *>(node->step.get()))
            return true;
        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return false;
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
