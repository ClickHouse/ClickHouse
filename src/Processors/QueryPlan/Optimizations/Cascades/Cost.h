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
};


class Memo;
class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

using GroupId = size_t;

class JoinStepLogical;
class ReadFromMergeTree;
class FilterStep;
class ExpressionStep;
class AggregatingStep;

class CostEstimator
{
public:
    explicit CostEstimator(Memo & memo_)
        : memo(memo_)
    {}

    ExpressionCost estimateCost(GroupExpressionPtr expression);

private:
    ExpressionCost estimateHashJoinCost(
        const JoinStepLogical & join_step,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & left_statistics,
        const ExpressionStatistics & right_statistics);

    ExpressionCost estimateReadCost(const ReadFromMergeTree & read_step, const ExpressionStatistics & this_step_statistics);

    ExpressionCost estimateAggregationCost(
        const AggregatingStep & aggregating_step,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & input_statistics);

    Memo & memo;
    LoggerPtr log = getLogger("CostEstimator");
};

}
