#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/IO/FragmentsRequest.h>
#include <QueryCoordination/PlanFragment.h>
#include <QueryCoordination/RemotePipelinesManager.h>
#include <Common/logger_useful.h>

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
        : remote_pipelines_manager(std::make_shared<RemotePipelinesManager>())
        , log(&Poco::Logger::get("Coordinator"))
        , fragments(fragments_)
        , context(context_)
        , query(query_)
    {
    }

    void scheduleExecuteDistributedPlan();

    std::shared_ptr<RemotePipelinesManager> remote_pipelines_manager;

private:
    String assignFragmentToHost();

    bool isUpToDate(const QualifiedTableName & table_name);

    void sendFragmentToDistributed(const String & local_shard_host);

    void sendExecuteQueryPipelines(const String & local_shard_host);

    std::unordered_map<FragmentID, FragmentRequest> buildFragmentRequest();

    Poco::Logger * log;

    const PlanFragmentPtrs & fragments;

    HostToFragments host_fragments;
    FragmentToHosts fragment_hosts;
    std::unordered_map<FragmentID , PlanFragmentPtr> id_fragment;

    // all dest
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    ContextMutablePtr context;

    String query;

//    bool is_subquery;
//
//    /// for query: select * from aaa where id in (select id from bbb),
//    /// two phases are scheduled separately, we need make them scheduled same hosts
//    std::vector<String> prepare_hosts;
};

}
