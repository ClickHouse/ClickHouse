#pragma once

#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/ScanStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/Statistics/Statistics.h>


namespace DB
{

class CostCalculator
{
public:
    CostCalculator(GroupNode & group_node_, const PhysicalProperties & prop_, const Statistics & statistics_, const std::vector<Statistics> & input_statistics_)
        : group_node(group_node_), prop(prop_), statistics(statistics_), input_statistics(input_statistics_)
    {
    }

    Float64 calcCost(ReadFromMergeTree & /*step*/)
    {
        /// TODO get rows by statistics
        return 1 * statistics.getOutputRowSize();
    }

    Float64 calcCost(AggregatingStep & step)
    {
        if (step.isFinal())
        {
            /// TODO get rows, cardinality by statistics
            if (prop.distribution.type == PhysicalProperties::DistributionType::Hashed)
            {
                return 6 * (input_statistics.front().getOutputRowSize() / 3/*shard_num*/); /// fake shard_num
            }
            else
                return 6 * (input_statistics.front().getOutputRowSize());
        }
        else
        {
            /// TODO get rows, cardinality by statistics
            return 3 * (input_statistics.front().getOutputRowSize() / 3/*shard_num*/);
        }
    }

    Float64 calcCost(MergingAggregatedStep & /*step*/)
    {
        /// TODO get rows, cardinality by statistics
        if (prop.distribution.type == PhysicalProperties::DistributionType::Hashed)
        {
            return 3 * (input_statistics.front().getOutputRowSize() / 3/*shard_num*/);
        }
        return 3 * input_statistics.front().getOutputRowSize();
    }

    Float64 calcCost(ExchangeDataStep & step)
    {
        /// TODO get rows, cardinality by statistics
        /// TODO by type
        if (step.getDistributionType() == PhysicalProperties::DistributionType::Replicated)
        {
            return 1 * (statistics.getOutputRowSize() * 3/*shard_num*/);
        }
        return 1 * statistics.getOutputRowSize();
    }

    Float64 calcCost(SortingStep & /*step*/)
    {
        if (prop.distribution.type == PhysicalProperties::DistributionType::Singleton)
        {
            return 3 * input_statistics.front().getOutputRowSize();
        }
        return 3 * (input_statistics.front().getOutputRowSize() / 3/*shard_num*/);
    }

    Float64 calcCost(JoinStep & /*step*/)
    {
        return 1;
    }

    Float64 calcCost(UnionStep & /*step*/)
    {
        return 1;
    }

    Float64 calcCost(LimitStep & step)
    {
        if (step.getStepDescription().contains("preliminary LIMIT"))
        {
            return 1 * (input_statistics.front().getOutputRowSize() / 3/*shard_num*/);
        }
        else if (step.getStepDescription().contains("final LIMIT"))
        {
            return 1 * input_statistics.front().getOutputRowSize();
        }
        else
        {
            return 1 * input_statistics.front().getOutputRowSize();
        }
    }

    Float64 calcCost()
    {
        QueryPlanStepPtr step = group_node.getStep();
        if (auto * scan_step = dynamic_cast<ReadFromMergeTree *>(step.get()))
        {
            return calcCost(*scan_step);
        }
        else if (auto * agg_step = dynamic_cast<AggregatingStep *>(step.get()))
        {
            return calcCost(*agg_step);
        }
        else if (auto * merge_agg_step = dynamic_cast<MergingAggregatedStep *>(step.get()))
        {
            return calcCost(*merge_agg_step);
        }
        else if (auto * sort_step = dynamic_cast<SortingStep *>(step.get()))
        {
            return calcCost(*sort_step);
        }
        else if (auto * join_step = dynamic_cast<JoinStep *>(step.get()))
        {
            return calcCost(*join_step);
        }
        else if (auto * union_step = dynamic_cast<UnionStep *>(step.get()))
        {
            return calcCost(*union_step);
        }
        else if (auto * exchange_step = dynamic_cast<ExchangeDataStep *>(step.get()))
        {
            return calcCost(*exchange_step);
        }
        else if (auto * limit_step = dynamic_cast<LimitStep *>(step.get()))
        {
            return calcCost(*limit_step);
        }

        //    else if (dynamic_cast<CreatingSetStep *>(root_node.step.get()))
        //    {
        //        /// Do noting, add to brother fragment
        //        result = child_fragments[0];
        //    }
        //    else if (dynamic_cast<CreatingSetsStep *>(root_node.step.get()))
        //    {
        //        /// CreatingSetsStep need push to child_fragments[0], connect child_fragments[0] to child_fragments[1-N]
        //        result = createCreatingSetsFragment(root_node, child_fragments);
        //    }
        //    else if (needPushDownChild(root_node.step))
        //    {
        //        /// not Projection ExpressionStep push it to child_fragments[0]
        //
        //        child_fragments[0]->addStep(root_node.step);
        //        result = child_fragments[0];
        //    }
        //    else if (isLimitRelated(root_node.step))
        //    {
        //        pushDownLimitRelated(root_node.step, child_fragments[0]);
        //        result = child_fragments[0];
        //    }
        return 1;
    }

private:
    GroupNode & group_node;
    const PhysicalProperties & prop;
    const Statistics & statistics;
    const std::vector<Statistics> & input_statistics;
};

}
