#include <Processors/QueryPlan/Optimizations/joinRuntimeFilterKeyPath.h>

#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

bool keyPathHasInputAbove(const QueryPlan::Node * node, const String & column, UInt64 limit)
{
    while (node)
    {
        if (const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get()))
        {
            const auto & join_operator = join->getJoinOperator();
            if (node->children.size() != 2 || join_operator.kind != JoinKind::Inner)
                return false;

            const bool from_left = node->children[0]->step->getOutputHeader()->has(column);
            if (!from_left
                && !(node->children[1]->step->getOutputHeader()->has(column)
                     && join_operator.strictness != JoinStrictness::Asof
                     && !typeid_cast<const JoinStepLogicalLookup *>(node->children[1]->step.get())))
                return false;

            if (auto rows = join->getInputRowsEstimation(from_left ? JoinTableSide::Left : JoinTableSide::Right); rows && *rows > limit)
                return true;
            node = node->children[from_left ? 0 : 1];
            continue;
        }

        if (node->children.size() != 1 || !node->children.front()->step->getOutputHeader()->has(column))
            return false;

        const ActionsDAG * dag = nullptr;
        if (const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get()))
            dag = &expression->getExpression();
        else if (const auto * filter = typeid_cast<const FilterStep *>(node->step.get()))
            dag = &filter->getExpression();
        else if (!typeid_cast<const BuildRuntimeFilterStep *>(node->step.get()))
            return false;

        if (dag && (dag->hasStatefulFunctions() || dag->hasArrayJoin()))
            return false;

        node = node->children.front();
    }
    return false;
}

}
