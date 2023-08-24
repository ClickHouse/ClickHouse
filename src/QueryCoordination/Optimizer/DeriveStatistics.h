#pragma once

#include <QueryCoordination/Optimizer/PlanStepVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

class DeriveStatistics : public PlanStepVisitor<Statistics>
{
public:
    using Base = PlanStepVisitor<Statistics>;

    explicit DeriveStatistics(const std::vector<Statistics> & input_statistics_) : input_statistics(input_statistics_) { }

    Statistics visit(QueryPlanStepPtr step) override;

    Statistics visitDefault() override;

    Statistics visit(ReadFromMergeTree & step) override;

    Statistics visit(AggregatingStep & step) override;

    Statistics visit(MergingAggregatedStep & step) override;

    Statistics visit(ExpressionStep & step) override;

    Statistics visit(SortingStep & step) override;

    Statistics visit(LimitStep & step) override;

    Statistics visit(JoinStep & step) override;

    Statistics visit(UnionStep & step) override;

    Statistics visit(ExchangeDataStep & step) override;

private:
    const std::vector<Statistics> & input_statistics;
};

}
