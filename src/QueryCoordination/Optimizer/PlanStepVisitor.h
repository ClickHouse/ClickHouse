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
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <Common/typeid_cast.h>

namespace DB
{

template <class R>
class PlanStepVisitor
{
public:
    using ResultType = R;

    virtual ~PlanStepVisitor() = default;

    virtual R visit(QueryPlanStepPtr step)
    {
        if (auto * scan_step = typeid_cast<ReadFromMergeTree *>(step.get()))
        {
            return visit(*scan_step);
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
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(step.get()))
        {
            return visit(*expression_step);
        }
        else
        {
            return visitDefault();
        }
    }

    /// default implement
    virtual R visitDefault()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: not implement");
    }

    virtual R visit(ReadFromMergeTree & /*step*/) { return visitDefault(); }

    virtual R visit(AggregatingStep & /*step*/) { return visitDefault(); }

    virtual R visit(MergingAggregatedStep & /*step*/) { return visitDefault(); }

    virtual R visit(ExpressionStep & /*step*/) { return visitDefault(); }

    virtual R visit(SortingStep & /*step*/) { return visitDefault(); }

    virtual R visit(LimitStep & /*step*/) { return visitDefault(); }

    virtual R visit(JoinStep & /*step*/) { return visitDefault(); }

    virtual R visit(UnionStep & /*step*/) { return visitDefault(); }

    virtual R visit(ExchangeDataStep & /*step*/) { return visitDefault(); }

};

}
