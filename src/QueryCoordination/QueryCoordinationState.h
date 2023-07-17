#pragma once

#include <QueryCoordination/Pipelines/Pipelines.h>
#include <QueryCoordination/Fragments/PlanFragment.h>

namespace DB
{

class QueryCoordinationState {

public:
    PlanFragmentPtrs fragments;
    Pipelines pipelines;
    std::unordered_map<String, IConnectionPool::Entry> remote_host_connection;
    StorageLimitsList storage_limits;
};


}
