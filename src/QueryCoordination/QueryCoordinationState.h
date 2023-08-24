#pragma once

#include <QueryCoordination/Pipelines/Pipelines.h>
#include <Client/ConnectionPool.h>

namespace DB
{

class PlanFragment;

using PlanFragmentPtr = std::shared_ptr<PlanFragment>;
using PlanFragmentPtrs = std::vector<PlanFragmentPtr>;

class QueryCoordinationState {

public:
    PlanFragmentPtrs fragments;
    Pipelines pipelines;
    std::unordered_map<String, IConnectionPool::Entry> remote_host_connection;
    StorageLimitsList storage_limits;
};


}
