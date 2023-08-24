//#pragma once
//
//#include <unordered_map>
//#include <string_view>
//
//#include <QueryCoordination/Fragments/PlanFragment.h>
//#include <QueryCoordination/IO/FragmentRequest.h>
//#include <QueryCoordination/IO/ExchangeDataRequest.h>
//#include <Core/Block.h>
//#include <QueryCoordination/PipelineExecutors.h>
//#include <QueryCoordination/ExchangeDataSource.h>
//#include <QueryCoordination/CompletedPipelinesExecutor.h>
//
//namespace DB
//{
//
//using ExchangeDataSources = std::unordered_map<String, std::shared_ptr<ExchangeDataSource>>;
//
//struct FragmentDistributed
//{
//    PlanFragmentPtr fragment;
//    Destinations data_to;
//    Sources data_from;
//
//    bool is_finished = false;
//
//    ExchangeDataSources receivers;
//
//    static String receiverKey(PlanID exchange_id, const String & source)
//    {
//        return source + "_" + toString(exchange_id);
//    }
//
//};
//
//class FragmentMgr
//{
//public:
//    // from InterpreterSelectQueryFragments
//    void addFragment(const String & query_id, PlanFragmentPtr fragment, ContextMutablePtr context_);
//
//    // Keep fragments that need to be executed by themselves
//    void fragmentsToDistributed(const String & query_id, const std::vector<FragmentRequest> & need_execute_fragments);
//
//    std::shared_ptr<ExchangeDataSource> findReceiver(const ExchangeDataRequest & exchange_data_request) const;
//
//    QueryPipeline findRootQueryPipeline(const String & query_id);
//
//    static FragmentMgr & getInstance()
//    {
//        static FragmentMgr fragment_mgr;
//        return fragment_mgr;
//    }
//
//    void onFinish(const String & query_id);
//
//    std::shared_ptr<CompletedPipelinesExecutor> createPipelinesExecutor(const String & query_id);
//
//private:
//    FragmentMgr() : log(&Poco::Logger::get("FragmentMgr")) { }
//
//    void fragmentsToQueryPipelines(const String & query_id);
//
//    struct Data
//    {
//        std::vector<FragmentDistributed> fragments_distributed;
//        std::vector<QueryPipeline> query_pipelines;
//
//        ContextMutablePtr query_context;
//
//        mutable std::mutex mutex;
//
//        void assignThreadNum()
//        {
//            std::vector<Float64> threads_weight;
//            Float64 total_weight = 0;
//
//            for (const auto & query_pipeline : query_pipelines)
//            {
//                Float64 weight = query_pipeline.getProcessors().size();
//                total_weight += weight;
//                threads_weight.emplace_back(weight);
//            }
//
//            size_t max_threads = query_context->getSettingsRef().max_threads;
//            for (size_t i = 0; i < query_pipelines.size(); ++i)
//            {
//                size_t num_threads = static_cast<size_t>((threads_weight[i] / total_weight) * max_threads);
//                query_pipelines[i].setNumThreads(num_threads);
//            }
//        }
//    };
//
//    std::shared_ptr<Data> find(const String & query_id) const;
//
//    using QueryFragment = std::unordered_map<String, std::shared_ptr<Data>>;
//
//    Poco::Logger * log;
//
//    std::unique_ptr<ThreadFromGlobalPool> cleaner;
//
//    QueryFragment query_fragment;
//    mutable std::mutex fragments_mutex;
//
//    PipelineExecutors executors;
//};
//
//}
