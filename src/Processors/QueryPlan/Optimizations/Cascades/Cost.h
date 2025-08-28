#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Common/Logger.h>
#include <base/types.h>
#include <memory>

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
    CostEstimator(const Memo & memo_, const IOptimizerStatistics & statistics_)
        : memo(memo_)
        , statistics(statistics_)
    {}

    ExpressionCost estimateCost(GroupExpressionPtr expression);

private:
    ExpressionCost estimateHashJoinCost(const JoinStepLogical & join_step, GroupId left_tree, GroupId right_tree);
    ExpressionCost estimateReadCost(const ReadFromMergeTree & read_step);

    const Memo & memo;
    const IOptimizerStatistics & statistics;
    LoggerPtr log = getLogger("CostEstimator");
};

}
