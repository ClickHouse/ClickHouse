#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/PlanFragment.h>
#include <Common/logger_useful.h>

namespace DB
{

// Need fragment container (fragment id and fragment) and receiver map. Data sink by fragment id and exchange id to find exchange data recvr.
// maybe by query id, fragment id, exchange id
class Coordinator
{
public:
    void schedule();

    const PlanFragmentPtrs & localFragments() const { return local_fragments; }

    Coordinator(const PlanFragmentPtrs & fragments_, ContextMutablePtr context_) : fragments(fragments_), context(context_) {}

private:
    const PlanFragmentPtrs & fragments;

    std::unordered_map<UInt32, std::vector<String>> fragment_id_hosts;

    // all dest
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    std::unordered_map<String, std::vector<PlanFragmentPtr>> host_fragment_ids;

    PlanFragmentPtrs local_fragments;

    ContextMutablePtr context;

//    Poco::Logger * log;
};

}
