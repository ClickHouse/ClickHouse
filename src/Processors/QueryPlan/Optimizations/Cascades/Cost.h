#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Common/Logger.h>
#include <base/types.h>
#include <limits>
#include <memory>

namespace DB
{

struct CostConfig
{
    Float64 cpu_weight = 1.0;
    Float64 memory_weight = 1.0;
    Float64 network_weight = 1.0;
    Float64 io_weight = 1.0;
    Float64 exchange_fixed_overhead = 100.0;

    String dump() const;
};

CostConfig parseCostConfig(const String & json_str);

struct Cost
{
    Float64 cpu = 0;
    Float64 memory = 0;
    Float64 network = 0;
    Float64 io = 0;
    Float64 wallclock_time = 0;

    Float64 total() const { return cpu + memory + network + io; }

    Float64 weighted_total(const CostConfig & config) const
    {
        return cpu * config.cpu_weight + memory * config.memory_weight
             + network * config.network_weight + io * config.io_weight;
    }

    static Cost infinity()
    {
        return Cost{
            .cpu = std::numeric_limits<Float64>::infinity(),
            .memory = std::numeric_limits<Float64>::infinity(),
            .network = std::numeric_limits<Float64>::infinity(),
            .io = std::numeric_limits<Float64>::infinity(),
            .wallclock_time = std::numeric_limits<Float64>::infinity(),
        };
    }

    Cost & operator+=(const Cost & other)
    {
        cpu += other.cpu;
        memory += other.memory;
        network += other.network;
        io += other.io;
        wallclock_time += other.wallclock_time;
        return *this;
    }
};

struct ExpressionCost
{
    Cost cost;          /// Cost of this expression only
    Cost subtree_cost;  /// Total cost of the whole subtree (this expression and all its children)
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

struct IJoinStrategy;
struct IAggregationStrategy;
struct IReadStrategy;

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
        const IJoinStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & left_statistics,
        const ExpressionStatistics & right_statistics,
        Float64 distribution_node_count);

    ExpressionCost estimateReadCost(
        const ReadFromMergeTree & read_step,
        const IReadStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        Float64 distribution_node_count);

    ExpressionCost estimateAggregationCost(
        const AggregatingStep & aggregating_step,
        const IAggregationStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & input_statistics,
        Float64 distribution_node_count);

    Memo & memo;
    LoggerPtr log = getLogger("CostEstimator");
};

}
