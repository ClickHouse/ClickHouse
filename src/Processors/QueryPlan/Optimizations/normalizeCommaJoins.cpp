#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Core/Joins.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{

extern const int INCORRECT_QUERY;

}

namespace DB::QueryPlanOptimizations
{

/// The planner keeps the `Comma` kind so that an unrewritten comma join can be told apart from an explicit
/// `CROSS JOIN`. By now `mergeFilterIntoJoinCondition` has turned every comma join with usable `WHERE`
/// equalities into `Inner`, so a join that is still `Comma` is a genuine cross product.
void normalizeCommaJoins(QueryPlan::Node & root, const QueryPlanOptimizationSettings & optimization_settings)
{
    Stack stack;
    traverseQueryPlan(stack, root, [&](QueryPlan::Node & node)
    {
        auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
        if (!join_step)
            return;

        auto & join_operator = join_step->getJoinOperator();
        if (join_operator.kind != JoinKind::Comma)
            return;

        if (optimization_settings.force_comma_join_rewrite)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Failed to rewrite '{}' to INNER JOIN: no equi-join conditions found in WHERE clause. "
                "You may set setting `cross_to_inner_join_rewrite` to `1` to allow slow CROSS JOIN for this case",
                join_step->getReadableRelationName());

        /// The join order optimizers only know `Cross`.
        join_operator.kind = JoinKind::Cross;
    });
}

}
