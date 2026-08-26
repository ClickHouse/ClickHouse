#include <Processors/QueryPlan/Optimizations/joinEquivalentSets.h>

#include <Processors/QueryPlan/JoinStepLogical.h>

namespace DB::QueryPlanOptimizations
{

std::vector<JoinActionRefPair> getJoiningKeysForJoinStep(const JoinOperator & join_operator)
{
    std::vector<JoinActionRefPair> joining_keys;
    for (const auto & predicate : join_operator.expression)
    {
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
            continue;

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;

        auto left_column = lhs.getColumn();
        auto right_column = rhs.getColumn();
        if (!left_column.type->equals(*right_column.type))
            continue;
        joining_keys.emplace_back(lhs, rhs);
    }
    return joining_keys;
}

}
