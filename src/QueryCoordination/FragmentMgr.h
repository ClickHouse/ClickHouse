#pragma once

#include <unordered_map>
#include <string_view>

#include <QueryCoordination/PlanFragment.h>
#include <QueryCoordination/IO/FragmentRequest.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>
#include <Core/Block.h>
#include <QueryCoordination/PipelineExecutors.h>
#include <QueryCoordination/ExchangeDataReceiver.h>

namespace DB
{

using ExchangeDataReceivers = std::vector<std::shared_ptr<ExchangeDataReceiver>>;

struct FragmentDistributed
{
    PlanFragmentPtr fragment;
    Destinations dests;
    ExchangeDataReceivers receivers;
};

class FragmentMgr
{
public:
    // from InterpreterSelectQueryFragments
    void addFragment(String query_id, PlanFragmentPtr fragment, ContextMutablePtr context_);

    // Keep fragments that need to be executed by themselves
    void fragmentsToDistributed(String query_id, const std::vector<FragmentRequest> & self_fragment);

    void executeQueryPipelines(String query_id);

    void receiveData(const ExchangeDataRequest & exchange_data_request, Block & block);

    QueryPipeline findRootQueryPipeline(String query_id);

    static FragmentMgr & getInstance()
    {
        static FragmentMgr fragment_mgr;
        return fragment_mgr;
    }

private:
    FragmentMgr() = default;

    void fragmentsToQueryPipelines(String query_id);

    void cleanerThread();

    struct Data
    {
        std::vector<FragmentDistributed> fragments_distributed;
        std::vector<QueryPipeline> query_pipelines;

        ContextMutablePtr query_context;
    };

    using QueryFragment = std::unordered_map<String, std::unique_ptr<Data>>;

    std::unique_ptr<ThreadFromGlobalPool> cleaner;

    QueryFragment query_fragment;
    mutable std::mutex fragments_mutex;

    PipelineExecutors executors;
};

}
