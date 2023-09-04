#pragma once

#include <Interpreters/ActionsDAG.h>


namespace DB
{

template <class R, class C>
class ActionsDAGVisitor
{
public:
    using ResultType = R;
    using ContextType = C;

    virtual ~ActionsDAGVisitor() = default;

    virtual R visit(ActionsDAG::ActionType step, C context);

    virtual R visit(ActionsDAG::Node step);

    /// default implement
    virtual R visitDefault()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: not implement");
    }

    virtual R visit(ReadFromMergeTree & /*step*/) { return visitDefault(); }

    virtual R visit(ExpressionStep & /*step*/) { return visitDefault(); }

    virtual R visit(FilterStep & /*step*/) { return visitDefault(); }

    virtual R visit(AggregatingStep & /*step*/) { return visitDefault(); }

    virtual R visit(MergingAggregatedStep & /*step*/) { return visitDefault(); }

    virtual R visit(SortingStep & /*step*/) { return visitDefault(); }

    virtual R visit(LimitStep & /*step*/) { return visitDefault(); }

    virtual R visit(JoinStep & /*step*/) { return visitDefault(); }

    virtual R visit(UnionStep & /*step*/) { return visitDefault(); }

    virtual R visit(ExchangeDataStep & /*step*/) { return visitDefault(); }

};

}


