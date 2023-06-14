#include <unordered_map>
#include <QueryCoordination/FragmentMgr.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/DataSink.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/Cluster.h>
#include <Common/setThreadName.h>


namespace DB
{

// for SECONDARY_QUERY from tcphandler, for INITIAL_QUERY from InterpreterSelectQueryFragments
void FragmentMgr::addFragment(String query_id, PlanFragmentPtr fragment, ContextMutablePtr context_)
{
    std::lock_guard lock(fragments_mutex);
    if (!query_fragment.contains(query_id))
    {
        auto data = std::make_unique<Data>();
        query_fragment.try_emplace(query_id, std::move(data));
    }
    auto & data = query_fragment[query_id];
    data->fragments_distributed.emplace_back(FragmentDistributed{.fragment = fragment, .dests = {}});
    data->query_context = context_;
}

// Keep fragments that need to be executed by themselves
void FragmentMgr::fragmentsToDistributed(String query_id, const std::vector<FragmentRequest> & self_fragment)
{
    std::unordered_map<UInt32, Destinations> to_keep_fragment_dests;
    for (const auto & request : self_fragment)
    {
        to_keep_fragment_dests.emplace(request.fragment_id, request.destinations);
    }

    std::lock_guard lock(fragments_mutex);
    auto & data = query_fragment[query_id];
    std::vector<FragmentDistributed> final_fragments;
    for (auto all_fragment_dests : data->fragments_distributed)
    {
        if (to_keep_fragment_dests.contains(all_fragment_dests.fragment->getFragmentId()))
        {
            final_fragments.emplace_back(FragmentDistributed{
                .fragment = all_fragment_dests.fragment, .dests = to_keep_fragment_dests[all_fragment_dests.fragment->getFragmentId()]});
        }
    }
    data->fragments_distributed = final_fragments;

    fragmentsToQueryPipelines(query_id);
}

void FragmentMgr::fragmentsToQueryPipelines(String query_id)
{
    auto & data = query_fragment[query_id];
    auto context = data->query_context;
    /// build query pipeline, find connections by dests list
    for (FragmentDistributed & fragments_distributed : data->fragments_distributed)
    {
        std::vector<DataSink::Channel> channels;

        for (const auto & shard_info : fragments_distributed.fragment->getCluster()->getShardsInfo())
        {
            auto current_settings = context->getSettingsRef();
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                                current_settings).getSaturated(
                                    current_settings.max_execution_time);

            bool is_local = shard_info.isLocal();

            auto connections = shard_info.pool->getMany(timeouts, &current_settings, PoolMode::GET_MANY);

            for (auto connection : connections)
            {
                if (std::count(fragments_distributed.dests.begin(), fragments_distributed.dests.end(), connection->getDescription()))
                {
                    channels.emplace_back(DataSink::Channel{.connection = connection, .is_local = is_local});
                    break;
                }
            }
        }

        QueryPipeline && pipeline = fragments_distributed.fragment->buildQueryPipeline(channels);

        // register ExchangeDataReceiver
        for (auto & processor : pipeline.getProcessors())
        {
            if (ExchangeDataReceiver * receiver = dynamic_cast<ExchangeDataReceiver *>(processor.get()))
            {
                // register ExchangeDataReceiver
                fragments_distributed.receivers.emplace_back(receiver->shared_from_this());
            }
        }

        data->query_pipelines.emplace_back(std::move(pipeline));
    }

}

void FragmentMgr::executeQueryPipelines(String query_id)
{
    /// begin execute pipeline
    std::lock_guard lock(fragments_mutex);
    auto & data = query_fragment[query_id];

    /// root fragment execute from tcphandler
    for (size_t i = 0; i < data->fragments_distributed.size(); ++i)
    {
        if (data->fragments_distributed[i].fragment->getDestFragment())
        {
            executors.execute(data->query_pipelines[i]);
        }
    }
}

QueryPipeline FragmentMgr::findRootQueryPipeline(String query_id)
{
    std::lock_guard lock(fragments_mutex);
    auto & data = query_fragment[query_id];
    for (size_t i = 0; i < data->fragments_distributed.size(); ++i)
    {
        if (!data->fragments_distributed[i].fragment->getDestFragment())
        {
            return std::move(data->query_pipelines[i]);
        }
    }

    throw;
}

void FragmentMgr::cleanerThread()
{
    // TODO lock
//    while (!shutdown)
//    {
//        for (auto it = executors.begin(); it != executors.end();)
//        {
//            if ((*it)->is_finished)
//            {
//                it = executors.erase(it);
//            }
//            else
//            {
//                it++;
//            }
//        }
//
//        /// TODO sleep
//    }
}

/// TODO when pipeline execute done remove fragment.

void FragmentMgr::receiveData(const ExchangeDataRequest & exchange_data_request, Block & block)
{
    std::lock_guard lock(fragments_mutex);
    auto it = query_fragment.find(exchange_data_request.query_id);
    if (it == query_fragment.end())
        throw;

    for (auto & fragment : it->second->fragments_distributed)
    {
        if (fragment.fragment->getFragmentId() == exchange_data_request.fragment_id)
        {
            for (auto receiver : fragment.receivers)
            {
                if (receiver->getPlanId() == exchange_data_request.exchange_id)
                {
                    receiver->receive(block);
                    break;
                }
            }
            break;
        }
    }
}

}
