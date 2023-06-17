#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/PlanFragment.h>
#include <Common/logger_useful.h>
#include <QueryCoordination/IO/FragmentsRequest.h>

namespace DB
{

using Hosts = std::vector<String>;

using HostToFragments = std::unordered_map<String, PlanFragmentPtrs>;
using FragmentToHosts = std::unordered_map<FragmentID , Hosts>;

// Need fragment container (fragment id and fragment) and receiver map. Data sink by fragment id and exchange id to find exchange data recvr.
// maybe by query id, fragment id, exchange id
class Coordinator
{
public:
    Coordinator(const PlanFragmentPtrs & fragments_, ContextMutablePtr context_, String query_)
        : fragments(fragments_), context(context_), query(query_)
    {
    }

    void scheduleExecuteDistributedPlan();

private:
    String assignFragmentToHost();

    void sendFragmentToDistributed(const String & local_shard_host);

    void sendExecuteQueryPipelines(const String & local_shard_host);

    std::unordered_map<FragmentID, FragmentRequest> buildFragmentRequest();

    const PlanFragmentPtrs & fragments;

    HostToFragments host_fragments;
    FragmentToHosts fragment_hosts;
    std::unordered_map<FragmentID , PlanFragmentPtr> id_fragment;

    // all dest
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    ContextMutablePtr context;

    String query;
//    Poco::Logger * log;
};

}
