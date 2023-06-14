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
    Coordinator(const PlanFragmentPtrs & fragments_, ContextMutablePtr context_) : fragments(fragments_), context(context_) {}

    void scheduleExecuteDistributedPlan();

private:
    void sendFragmentToDistributed(const PoolBase<DB::Connection>::Entry & local_shard_connection);

    void sendExecuteQueryPipelines(const PoolBase<DB::Connection>::Entry & local_shard_connection);

private:
    const PlanFragmentPtrs & fragments;

    std::unordered_map<FragmentID, std::vector<String>> fragment_id_hosts;

    // all dest
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    std::unordered_map<String, std::vector<PlanFragmentPtr>> host_fragment_ids;

    ContextMutablePtr context;

    String query;
//    Poco::Logger * log;
};

}
