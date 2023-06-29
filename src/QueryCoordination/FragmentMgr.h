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

using ExchangeDataReceivers = std::unordered_map<String, std::shared_ptr<ExchangeDataReceiver>>;

struct FragmentDistributed
{
    PlanFragmentPtr fragment;
    Destinations data_to;
    Sources data_from;

    bool is_finished = false;

    ExchangeDataReceivers receivers;

    static String receiverKey(PlanID exchange_id, const String & source)
    {
        return source + "_" + toString(exchange_id);
    }

};

class FragmentMgr
{
public:
    // from InterpreterSelectQueryFragments
    void addFragment(const String & query_id, PlanFragmentPtr fragment, ContextMutablePtr context_);

    // Keep fragments that need to be executed by themselves
    void fragmentsToDistributed(const String & query_id, const std::vector<FragmentRequest> & need_execute_fragments);

    void executeQueryPipelines(const String & query_id);

    std::shared_ptr<ExchangeDataReceiver> findReceiver(const ExchangeDataRequest & exchange_data_request) const;

    void rootQueryPipelineFinish(const String & query_id);

    QueryPipeline findRootQueryPipeline(const String & query_id);

    ContextMutablePtr findQueryContext(const String & query_id);

    static FragmentMgr & getInstance()
    {
        static FragmentMgr fragment_mgr;
        return fragment_mgr;
    }

    void onFinish(const String & query_id, FragmentID fragment_id);

private:
    FragmentMgr() : log(&Poco::Logger::get("FragmentMgr")) { }

    void fragmentsToQueryPipelines(const String & query_id);

    struct Data
    {
        std::vector<FragmentDistributed> fragments_distributed;
        std::vector<QueryPipeline> query_pipelines;

        ContextMutablePtr query_context;

        mutable std::mutex mutex;
    };

    std::shared_ptr<Data> find(const String & query_id) const;

    using QueryFragment = std::unordered_map<String, std::shared_ptr<Data>>;

    Poco::Logger * log;

    std::unique_ptr<ThreadFromGlobalPool> cleaner;

    QueryFragment query_fragment;
    mutable std::mutex fragments_mutex;

    PipelineExecutors executors;
};

}
