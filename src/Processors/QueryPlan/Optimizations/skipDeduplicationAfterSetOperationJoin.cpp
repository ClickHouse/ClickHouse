#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

/// `EXCEPT DISTINCT` as LEFT ANTI emits every probe row whose key the build side does not have, including the
/// duplicates. Swapped to RIGHT ANTI, it emits each key once, so its `DISTINCT` only has to enforce the limit on rows.
void skipDeduplicationAfterSetOperationJoin(QueryPlan::Node & node)
{
    auto * distinct = typeid_cast<DistinctStep *>(node.step.get());
    if (!distinct || !distinct->deduplicatesExceptJoin())
        return;

    /// The planner puts only renaming between the join and the `DISTINCT`, and filters pushed down keep the rows distinct.
    const QueryPlan::Node * child = node.children.front();
    while (typeid_cast<const ExpressionStep *>(child->step.get()) || typeid_cast<const FilterStep *>(child->step.get()))
        child = child->children.front();

    const auto * join_step = typeid_cast<const JoinStep *>(child->step.get());
    if (join_step && join_step->getJoin()->getTableJoin().kind() == JoinKind::Right)
        distinct->skipDeduplication();
}

}
