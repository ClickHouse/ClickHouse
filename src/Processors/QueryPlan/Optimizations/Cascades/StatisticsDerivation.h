#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Common/Logger.h>

namespace DB
{

class Memo;
class JoinStepLogical;
class ReadFromMergeTree;
class FilterStep;
class ExpressionStep;
class AggregatingStep;

/// Derives statistics for groups in the Cascades optimizer.
/// Statistics are logical properties that describe the data (row counts, NDVs)
/// and should be available before cost estimation and rule application.
class StatisticsDerivation
{
public:
    explicit StatisticsDerivation(Memo & memo_, const IOptimizerStatistics & statistics_lookup_)
        : memo(memo_)
        , statistics_lookup(statistics_lookup_)
    {}

    /// Derive statistics for a group based on one of its logical expressions.
    /// This should be called when a group is explored and all its input groups have statistics.
    void deriveStatistics(GroupId group_id);

private:
    ExpressionStatistics deriveJoinStatistics(const JoinStepLogical & join_step, const ExpressionStatistics & left_statistics, const ExpressionStatistics & right_statistics);
    ExpressionStatistics deriveReadStatistics(const ReadFromMergeTree & read_step);
    ExpressionStatistics deriveFilterStatistics(const FilterStep & filter_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveAggregatingStatistics(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics);

    Memo & memo;
    const IOptimizerStatistics & statistics_lookup;
    LoggerPtr log = getLogger("StatisticsDerivation");
};

}
