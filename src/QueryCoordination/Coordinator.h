#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Fragments/FragmentRequest.h>
#include <Common/logger_useful.h>

namespace DB
{

using Hosts = std::vector<String>;
using FragmentID = Int32;

using HostToFragments = std::unordered_map<String, FragmentPtrs>;
using FragmentToHosts = std::unordered_map<FragmentID, Hosts>;


class Coordinator
{
public:
    Coordinator(const FragmentPtrs & fragments_, ContextMutablePtr context_, String query_);

    void schedulePrepareDistributedPipelines();

    std::unordered_map<String, IConnectionPool::Entry> getRemoteHostConnection();

    Pipelines pipelines;

private:
    void assignFragmentToHost();

    std::unordered_map<UInt32, std::vector<String>> assignSourceFragment();

    PoolBase<DB::Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name);
    PoolBase<DB::Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info);

    bool isUpToDate(const QualifiedTableName & table_name);

    void sendFragmentsToPreparePipelines();
    void sendBeginExecutePipelines();

    std::unordered_map<UInt32, FragmentRequest> buildFragmentRequest();

    Poco::Logger * log;

    const FragmentPtrs & fragments;

    HostToFragments host_fragments;
    FragmentToHosts fragment_hosts;
    std::unordered_map<UInt32, FragmentPtr> id_fragment;

    // all dest
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    ContextMutablePtr context;

    String query;

    String local_host;
};

}
