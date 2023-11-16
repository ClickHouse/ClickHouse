#pragma once

#include <Interpreters/TableJoin.h>
#include <Optimizer/GroupNode.h>
#include <Optimizer/PhysicalProperties.h>
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
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TopNStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

template <class R>
class PlanStepVisitor
{
public:
    using ResultType = R;

    virtual ~PlanStepVisitor() = default;

    virtual R visit(QueryPlanStepPtr step)
    {
        if (auto * scan_step = typeid_cast<ReadFromMergeTree *>(step.get()))
            return visit(*scan_step);
        else if (auto * agg_step = typeid_cast<AggregatingStep *>(step.get()))
            return visit(*agg_step);
        else if (auto * merge_agg_step = typeid_cast<MergingAggregatedStep *>(step.get()))
            return visit(*merge_agg_step);
        else if (auto * sort_step = typeid_cast<SortingStep *>(step.get()))
            return visit(*sort_step);
        else if (auto * topn_step = typeid_cast<TopNStep *>(step.get()))
            return visit(*topn_step);
        else if (auto * join_step = typeid_cast<JoinStep *>(step.get()))
            return visit(*join_step);
        else if (auto * union_step = typeid_cast<UnionStep *>(step.get()))
            return visit(*union_step);
        else if (auto * exchange_step = typeid_cast<ExchangeDataStep *>(step.get()))
            return visit(*exchange_step);
        else if (auto * limit_step = typeid_cast<LimitStep *>(step.get()))
            return visit(*limit_step);
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(step.get()))
            return visit(*expression_step);
        else if (auto * filter_step = typeid_cast<FilterStep *>(step.get()))
            return visit(*filter_step);
        else if (auto * creating_set_step = typeid_cast<CreatingSetStep *>(step.get()))
            return visit(*creating_set_step);
        else if (auto * creating_sets_step = typeid_cast<CreatingSetsStep *>(step.get()))
            return visit(*creating_sets_step);
        else if (auto * extremes_Step = typeid_cast<ExtremesStep *>(step.get()))
            return visit(*extremes_Step);
        else if (auto * rollup_step = typeid_cast<RollupStep *>(step.get()))
            return visit(*rollup_step);
        else if (auto * cube_step = typeid_cast<CubeStep *>(step.get()))
            return visit(*cube_step);
        else if (auto * totals_having_step = typeid_cast<TotalsHavingStep *>(step.get()))
            return visit(*totals_having_step);
        else if (auto * distinct_step = typeid_cast<DistinctStep *>(step.get()))
            return visit(*distinct_step);
        else
            return visitDefault(*step);
    }

    /// default implement
    virtual R visitDefault(IQueryPlanStep & /*step*/) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented."); }

    virtual R visit(ReadFromMergeTree & step) { return visitDefault(step); }

    virtual R visit(AggregatingStep & step) { return visitDefault(step); }

    virtual R visit(MergingAggregatedStep & step) { return visitDefault(step); }

    virtual R visit(ExpressionStep & step) { return visitDefault(step); }

    virtual R visit(FilterStep & step) { return visitDefault(step); }

    virtual R visit(SortingStep & step) { return visitDefault(step); }

    virtual R visit(LimitStep & step) { return visitDefault(step); }

    virtual R visit(JoinStep & step) { return visitDefault(step); }

    virtual R visit(UnionStep & step) { return visitDefault(step); }

    virtual R visit(ExchangeDataStep & step) { return visitDefault(step); }

    virtual R visit(CreatingSetStep & step) { return visitDefault(step); }

    virtual R visit(CreatingSetsStep & step) { return visitDefault(step); }

    virtual R visit(ExtremesStep & step) { return visitDefault(step); }

    virtual R visit(RollupStep & step) { return visitDefault(step); }

    virtual R visit(CubeStep & step) { return visitDefault(step); }

    virtual R visit(TotalsHavingStep & step) { return visitDefault(step); }

    virtual R visit(TopNStep & step) { return visitDefault(step); }

    virtual R visit(DistinctStep & step) { return visitDefault(step); }
};

}
