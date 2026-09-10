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

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get()))
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
