#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/IO/FragmentsRequest.h>
//#include <QueryCoordination/Fragments/PlanFragment.h>
#include <Common/logger_useful.h>

namespace DB
{

using Hosts = std::vector<String>;
using FragmentID = Int32;

using HostToFragments = std::unordered_map<String, PlanFragmentPtrs>;
using FragmentToHosts = std::unordered_map<FragmentID , Hosts>;


class Coordinator
{
public:
    Coordinator(const PlanFragmentPtrs & fragments_, ContextMutablePtr context_, String query_)
        : log(&Poco::Logger::get("Coordinator"))
        , fragments(fragments_)
        , context(context_)
        , query(query_)
    {
    }

    void schedulePrepareDistributedPipelines();

    std::unordered_map<String, IConnectionPool::Entry> getRemoteHostConnection();

    Pipelines pipelines;

private:
    String assignFragmentToHost();

    bool isUpToDate(const QualifiedTableName & table_name);

    void sendFragmentsToPreparePipelines();

    void sendBeginExecutePipelines();

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

    String local_host;
};

}
