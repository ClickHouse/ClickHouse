#include <Processors/QueryPlan/ParallelReplicasSplitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Processors/QueryPlan/ReadFromParallelReplicas.h>
#include <Processors/QueryPlan/QueryPlanVisitor.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>

namespace DB
{
namespace QueryPlanOptimizations
{

constexpr bool debug_logging_enabled = true;

class ApplyParallelReplicasVisitor : public QueryPlanVisitor<ApplyParallelReplicasVisitor, debug_logging_enabled>
{
    QueryPlan::Nodes & nodes;

public:
    explicit ApplyParallelReplicasVisitor(QueryPlan::Node * root_, QueryPlan::Nodes & nodes_)
        : QueryPlanVisitor<ApplyParallelReplicasVisitor, debug_logging_enabled>(root_)
        , nodes(nodes_)
    {
    }

    bool visitTopDownImpl(QueryPlan::Node *, QueryPlan::Node *) { return true; }

    void visitBottomUpImpl(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        auto * original_split_step = typeid_cast<ParallelReplicasSplitStep*>(current_node->step.get());
        if (!original_split_step)
            return;

        auto * original_split_node = current_node;
        const auto * parent_step = parent_node->step.get();

        if (typeid_cast<const ExpressionStep *>(parent_step) || typeid_cast<const FilterStep *>(parent_step)
            || typeid_cast<const BuildRuntimeFilterStep *>(parent_step))
        {
            /// swap steps
            std::swap(current_node->step, parent_node->step);
            return;
        }

        const auto * aggregating_step = typeid_cast<const AggregatingStep *>(parent_step);
        if (aggregating_step)
        {
            /// Params will be used by merge step
            Aggregator::Params aggregator_params = aggregating_step->getParams();
            GroupingSetsParamsList grouping_sets_params = aggregating_step->getGroupingSetsParamsList();

            const bool should_produce_results_in_order_of_bucket_number = aggregating_step->shouldProduceResultsInBucketOrder();
            const bool memory_bound_merging_of_aggregation_results_enabled = aggregating_step->usingMemoryBoundMerging();
            const bool original_step_was_final
                = aggregating_step->getFinal(); /// Save whether the original AggregatingStep was final or partial

            /// Convert Aggregation step to partial aggregation
            auto & partial_aggregation_node = nodes.emplace_back();
            partial_aggregation_node.step = aggregating_step->clone();
            typeid_cast<AggregatingStep *>(partial_aggregation_node.step.get())->setFinal(false);
            partial_aggregation_node.step->setStepDescription("partial");
            partial_aggregation_node.children = {original_split_node->children.front()};

            /// Add gather
            auto & new_split_node = nodes.emplace_back();
            new_split_node.step = std::make_unique<ParallelReplicasSplitStep>(partial_aggregation_node.step->getOutputHeader(), original_split_step->getContext());
            new_split_node.children = {&partial_aggregation_node};

            /// Replace original aggregation step with MergingAggregated step
            aggregator_params.only_merge = true; /// Merge partial aggregation results
            const bool memory_efficient_aggregation = false;
            QueryPlanStepPtr final_aggregation_step = std::make_unique<MergingAggregatedStep>(
                new_split_node.step->getOutputHeader(),
                aggregator_params,
                grouping_sets_params,
                /* final */ original_step_was_final,
                memory_efficient_aggregation,
                aggregating_step->getTemporaryDataMergeThreads(),
                should_produce_results_in_order_of_bucket_number,
                aggregating_step->getMaxBlockSize(),
                aggregating_step->getMaxBlockSizeForAggregationInOrder(),
                memory_bound_merging_of_aggregation_results_enabled);

            final_aggregation_step->setStepDescription("merge");
            parent_node->step = std::move(final_aggregation_step);
            parent_node->children = {&new_split_node};
            return;
        }
    }
};

void applyParallelReplicas(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & );

void applyParallelReplicas(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    if (!settings.enable_parallel_replicas)
        return;

    ApplyParallelReplicasVisitor(&node, nodes).visit();
}

}

}
