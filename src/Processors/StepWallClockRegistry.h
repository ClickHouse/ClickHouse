#pragma once

#include <atomic>
#include <memory>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <boost/container_hash/hash.hpp>

namespace DB
{

class StepWallClock;

class StepWallClockRegistry
{
public:
    StepWallClockRegistry();

    /// `only_built_child_plans` keeps the walk from asking a step to build child plans it does not
    /// already hold, which would make attaching the clocks change the work the query does. `EXPLAIN`
    /// wants those plans and so passes false.
    void populateFromPlan(const QueryPlan & plan, bool only_built_child_plans);

    StepWallClock * find(const String & step_uniq_id, size_t group);

    void markExecutionFinished();

    UInt64 getExecutionTimeNs() const { return execution_time_ns.load(std::memory_order_acquire); }

private:

    using StepAndGroup = std::pair<String, size_t>;
    using Hash = boost::hash<StepAndGroup>;
    using StepWallClockPtr = std::unique_ptr<StepWallClock>;
    using MapStepToWallClock = std::unordered_map<StepAndGroup, StepWallClockPtr, Hash>;

    UInt64 query_start_ns = 0;
    std::atomic<UInt64> execution_time_ns = 0;
    MapStepToWallClock clocks;
};
}
