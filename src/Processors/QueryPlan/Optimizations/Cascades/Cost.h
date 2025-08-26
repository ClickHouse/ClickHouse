#pragma once

#include <memory>
#include <base/types.h>

namespace DB
{

using Cost = Int64;

struct ExpressionCost
{
    Cost subtree_cost;
    UInt64 number_of_rows;
    /// TODO: number of distict values, histograms?
};


class Memo;
class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

using GroupId = size_t;

class JoinStep;


class CostEstimator
{
public:
    explicit CostEstimator(const Memo & memo_)
        : memo(memo_)
    {}

    ExpressionCost estimateCost(GroupExpressionPtr expression);

private:
    ExpressionCost estimateHashJoinCost(const JoinStep & join_step, GroupId left_tree, GroupId right_tree);

    const Memo & memo;
};

}
