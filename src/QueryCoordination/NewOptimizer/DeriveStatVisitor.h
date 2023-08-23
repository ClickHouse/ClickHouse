#pragma once

#include <QueryCoordination/NewOptimizer/Statistics/Statistics.h>
#include <QueryCoordination/NewOptimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveStatVisitor : public PlanStepVisitor<Statistics>
{
public:
    using Base = PlanStepVisitor<Statistics>;

    explicit DeriveStatVisitor(const std::vector<Statistics> & input_statistics_) : input_statistics(input_statistics_) { }

    Statistics visit(QueryPlanStepPtr step) override
    {
        return Base::visit(step);
    }

    Statistics visitStep() override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size);
        return statistics;
    }

    Statistics visit(ReadFromMergeTree & step) override
    {
        Statistics statistics;
        statistics.setOutputRowSize(step.getAnalysisResult().selected_rows);
        return statistics;
    }

    Statistics visit(AggregatingStep & step) override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;

        if (step.isFinal())
        {
            statistics.setOutputRowSize(std::max(size_t(1), input_row_size / 4)); /// fake agg
        }
        else
        {
            statistics.setOutputRowSize(std::max(size_t(1), input_row_size / 2));
        }
        return statistics;
    }

    Statistics visit(MergingAggregatedStep & /*step*/) override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size / 2);
        return statistics;
    }

    Statistics visit(ExpressionStep & /*step*/) override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size); /// TODO filter ?
        return statistics;
    }

    Statistics visit(SortingStep & step) override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        if (step.getLimit())
        {
            statistics.setOutputRowSize(std::min(input_row_size, size_t(step.getLimit())));
        }
        else
        {
            statistics.setOutputRowSize(input_row_size);
        }
        return statistics;
    }


    Statistics visit(LimitStep & step) override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        if (step.getLimit())
        {
            statistics.setOutputRowSize(std::min(input_row_size, size_t(step.getLimit())));
        }
        else
        {
            statistics.setOutputRowSize(input_row_size);
        }
        return statistics;
    }

    Statistics visit(JoinStep & /*step*/) override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        /// TODO inner join, cross join
        Statistics statistics;
        statistics.setOutputRowSize(input_row_size);
        return statistics;
    }

    Statistics visit(UnionStep & /*step*/) override
    {
        size_t input_row_size = 0;
        for (const auto & input_statistic : input_statistics)
        {
            input_row_size += input_statistic.getOutputRowSize();
        }

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size);
        return statistics;
    }

    Statistics visit(ExchangeDataStep & /*step*/) override
    {
        size_t input_row_size = input_statistics.front().getOutputRowSize();

        Statistics statistics;
        statistics.setOutputRowSize(input_row_size);
        return statistics;
    }

private:
    GroupNode node;
    const std::vector<Statistics> & input_statistics;
};

}
