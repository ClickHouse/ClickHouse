#pragma once

#include <QueryCoordination/NewOptimizer/Statistics/Statistics.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
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
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <QueryCoordination/NewOptimizer/GroupNode.h>

namespace DB
{

class DerivationStatistics
{
public:
    DerivationStatistics(GroupNode & group_node_, const std::vector<Statistics> & input_statistics_)
        : group_node(group_node_), input_statistics(input_statistics_)
    {
    }

    Statistics derivationStatistics(ReadFromMergeTree & step)
    {
        Statistics statistics;
        statistics.setOutputRowSize(step.getSelectedRows());
        return statistics;
    }

    Statistics derivationStatistics(AggregatingStep & step)
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;

        if (step.isFinal())
        {
            statistics.setOutputRowSize(input_row_size / 4); /// fake agg
        }
        else
        {
            statistics.setOutputRowSize(input_row_size / 2);
        }
        return statistics;
    }

    Statistics derivationStatistics(MergingAggregatedStep & /*step*/)
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size / 2);
        return statistics;
    }

    Statistics derivationStatistics(ExpressionStep & /*step*/)
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size); /// TODO filter ?
        return statistics;
    }

    Statistics derivationStatistics(SortingStep & step)
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        if (step.getLimit())
        {
            statistics.setOutputRowSize(step.getLimit());
        }
        else
        {
            statistics.setOutputRowSize(input_row_size);
        }
        return statistics;
    }


    Statistics derivationStatistics(LimitStep & step)
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        if (step.getLimit())
        {
            statistics.setOutputRowSize(step.getLimit());
        }
        else
        {
            statistics.setOutputRowSize(input_row_size);
        }
        return statistics;
    }

    Statistics derivationStatistics(JoinStep & /*step*/)
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        /// TODO inner join, cross join
        Statistics statistics;
        statistics.setOutputRowSize(input_row_size);
        return statistics;
    }

    Statistics derivationStatistics(UnionStep & /*step*/)
    {
        size_t input_row_size = 0;
        for (auto & input_statistic : input_statistics)
        {
            input_row_size += input_statistic.getOutputRowSize();
        }

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size);
        return statistics;
    }

    Statistics derivationStatistics(ExchangeDataStep & /*step*/)
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size);
        return statistics;
    }

    Statistics derivationStatistics()
    {
        if (auto * scan_step = dynamic_cast<ReadFromMergeTree *>(group_node.getStep().get()))
        {
            return derivationStatistics(*scan_step);
        }
        else if (auto * agg_step = dynamic_cast<AggregatingStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*agg_step);
        }
        else if (auto * merge_agg_step = dynamic_cast<MergingAggregatedStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*merge_agg_step);
        }
        else if (auto * sort_step = dynamic_cast<SortingStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*sort_step);
        }
        else if (auto * join_step = dynamic_cast<JoinStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*join_step);
        }
        else if (auto * union_step = dynamic_cast<UnionStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*union_step);
        }
        else if (auto * exchange_step = dynamic_cast<ExchangeDataStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*exchange_step);
        }
        else if (auto * limit_step = dynamic_cast<LimitStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*limit_step);
        }
        else if (auto * expression_step = dynamic_cast<ExpressionStep *>(group_node.getStep().get()))
        {
            return derivationStatistics(*expression_step);
        }
        else
        {
            size_t input_row_size = input_statistics.front().getOutputRowSize();

            Statistics statistics;
            statistics.setOutputRowSize(input_row_size);
            return statistics;
        }
    }

private:
    GroupNode & group_node;
    const std::vector<Statistics> & input_statistics;
};

}
