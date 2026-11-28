#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

void disableCompilationOverAtMostOneRow(QueryPlan::Node & root)
{
    UnorderedMapWithMemoryTracking<const QueryPlan::Node *, bool> at_most_one_row;
    Stack stack;

    /// A node is left after its children, so the walk from it stops at its child.
    traverseQueryPlan(stack, root, NoOp{}, [&](QueryPlan::Node & node)
    {
        at_most_one_row.emplace(&node, isUniqueByColumns(node, {}, &at_most_one_row));

        auto * expression = typeid_cast<ExpressionStep *>(node.step.get());
        if (expression && node.children.size() == 1 && !expression->getExpression().hasArrayJoin()
            && at_most_one_row.at(node.children.front()))
            expression->disableCompilation();
    });
}

}
