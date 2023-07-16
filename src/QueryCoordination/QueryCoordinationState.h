#pragma once

#include <QueryCoordination/Pipelines/Pipelines.h>

namespace DB
{

class QueryCoordinationState {
    PlanFragmentPtrs fragments;
    Pipelines pipelines;
    std::unordered_map<String, IConnectionPool::Entry> remote_host_connection;
};


}
