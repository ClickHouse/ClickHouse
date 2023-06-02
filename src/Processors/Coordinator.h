#pragma once

#include <Processors/QueryPlan/PlanFragment.h>
#include <Interpreters/Cluster.h>
#include <Common/logger_useful.h>

namespace DB
{

class Coordinator
{
public:
    void schedule();

    Coordinator(const PlanFragmentPtrs & fragments_, ContextMutablePtr context_) : fragments(fragments_), context(context_) {}

private:
    const PlanFragmentPtrs & fragments;

    std::unordered_map<UInt32, std::vector<String>> fragment_id_hosts;

    // all dest
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    std::unordered_map<String, std::vector<PlanFragmentPtr>> host_fragment_ids;

    ContextMutablePtr context;

//    Poco::Logger * log;
};

}
