#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

void disableCompilationOverAtMostOneRow(QueryPlan::Node & node)
{
    auto * expression = typeid_cast<ExpressionStep *>(node.step.get());
    if (!expression || node.children.size() != 1 || expression->getExpression().hasArrayJoin())
        return;

    if (isUniqueByColumns(*node.children.front(), {}))
        expression->disableCompilation();
}

}
