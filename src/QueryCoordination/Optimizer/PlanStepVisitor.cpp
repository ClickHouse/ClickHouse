#include "PlanStepVisitor.h"

namespace DB
{

template <class R>
R PlanStepVisitor<R>::visit(QueryPlanStepPtr step)
{
    if (auto * scan_step = typeid_cast<ReadFromMergeTree *>(step.get()))
    {
        return visit(*scan_step);
    }
    else if (auto * expression_step = typeid_cast<ExpressionStep *>(step.get()))
    {
        return visit(*expression_step);
    }
    else if (auto * filter_step = typeid_cast<FilterStep *>(step.get()))
    {
        return visit(*filter_step);
    }
    else if (auto * agg_step = typeid_cast<AggregatingStep *>(step.get()))
    {
        return visit(*agg_step);
    }
    else if (auto * merge_agg_step = typeid_cast<MergingAggregatedStep *>(step.get()))
    {
        return visit(*merge_agg_step);
    }
    else if (auto * sort_step = typeid_cast<SortingStep *>(step.get()))
    {
        return visit(*sort_step);
    }
    else if (auto * join_step = typeid_cast<JoinStep *>(step.get()))
    {
        return visit(*join_step);
    }
    else if (auto * union_step = typeid_cast<UnionStep *>(step.get()))
    {
        return visit(*union_step);
    }
    else if (auto * exchange_step = typeid_cast<ExchangeDataStep *>(step.get()))
    {
        return visit(*exchange_step);
    }
    else if (auto * limit_step = typeid_cast<LimitStep *>(step.get()))
    {
        return visit(*limit_step);
    }
    else
    {
        return visitDefault();
    }
}

}
