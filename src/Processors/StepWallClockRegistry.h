#pragma once

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

    void populateFromPlan(const QueryPlan & plan);

    StepWallClock * find(const IQueryPlanStep *, size_t group) const;
private:

    using StepAndGroup = std::pair<const IQueryPlanStep *, size_t>;
    using Hash = boost::hash<std::pair<const IQueryPlanStep *, size_t>>;
    using StepWallClockPtr = std::unique_ptr<StepWallClock>;
    using MapStepToWallClock = std::unordered_map<StepAndGroup, StepWallClockPtr, Hash>;

    UInt64 query_start_ns = 0;
    MapStepToWallClock clocks;
};
}
