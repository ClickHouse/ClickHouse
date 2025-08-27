#pragma once

#include <memory>
#include <base/types.h>

namespace DB
{

using Cost = Float64;

struct ExpressionCost
{
    Cost subtree_cost = 0;
    Float64 number_of_rows = 0;
    /// TODO: number of distinct values, histograms?
};


class Memo;
class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

using GroupId = size_t;

class JoinStepLogical;
class ReadFromMergeTree;

class CostEstimator
{
public:
    explicit CostEstimator(const Memo & memo_)
        : memo(memo_)
    {}

    ExpressionCost estimateCost(GroupExpressionPtr expression);

private:
    ExpressionCost estimateHashJoinCost(const JoinStepLogical & join_step, GroupId left_tree, GroupId right_tree);
    ExpressionCost estimateReadCost(const ReadFromMergeTree & read_step);

    const Memo & memo;
};

}
