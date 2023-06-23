#include <unordered_map>
#include <QueryCoordination/FragmentMgr.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryCoordination/DataSink.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/Cluster.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>


namespace DB
{

std::shared_ptr<FragmentMgr::Data> FragmentMgr::find(const String & query_id) const
{
    std::lock_guard lock(fragments_mutex);
    if (!query_fragment.contains(query_id))
    {
        return nullptr;
    }
    return query_fragment.find(query_id)->second;
}

ContextMutablePtr FragmentMgr::findQueryContext(const String & query_id)
{
    auto data = find(query_id);
    return data->query_context;
}

// for SECONDARY_QUERY from tcphandler, for INITIAL_QUERY from InterpreterSelectQueryFragments
void FragmentMgr::addFragment(const String & query_id, PlanFragmentPtr fragment, ContextMutablePtr context_)
{
    std::shared_ptr<Data> data = find(query_id);
    if (!data)
    {
        data = std::make_shared<Data>();

        std::lock_guard lock(fragments_mutex);
        query_fragment.try_emplace(query_id, data);
    }

    std::lock_guard lock(data->mutex);
    data->fragments_distributed.emplace_back(FragmentDistributed{.fragment = fragment});
    data->query_context = context_;
}

// Keep fragments that need to be executed by themselves
void FragmentMgr::fragmentsToDistributed(const String & query_id, const std::vector<FragmentRequest> & need_execute_plan_fragments)
{
    std::unordered_map<FragmentID, FragmentRequest> need_execute_fragments;
    for (const auto & request : need_execute_plan_fragments)
    {
        need_execute_fragments.emplace(request.fragment_id, request);
    }

    auto data = find(query_id);

    {
        std::lock_guard lock(data->mutex);
        std::vector<FragmentDistributed> final_fragments;
        auto & all_fragments = data->fragments_distributed;
        for (const auto & all_fragment : all_fragments)
        {
            auto it = need_execute_fragments.find(all_fragment.fragment->getFragmentId());
            if (it != need_execute_fragments.end())
            {
                auto & request = it->second;
                final_fragments.emplace_back(FragmentDistributed{
                    .fragment = all_fragment.fragment, .data_to = request.data_to, .data_from = request.data_from});
            }
        }
        data->fragments_distributed = final_fragments;
    }

    fragmentsToQueryPipelines(query_id);
}

void FragmentMgr::fragmentsToQueryPipelines(const String & query_id)
{
    auto data = find(query_id);

    std::lock_guard lock(data->mutex);
    auto context = data->query_context;
    /// build query pipeline, find connections by dests list
    for (FragmentDistributed & fragments_distributed : data->fragments_distributed)
    {
        /// for data sink
        std::vector<DataSink::Channel> channels;
        String local_host;
        for (const auto & shard_info : fragments_distributed.fragment->getCluster()->getShardsInfo())
        {
            auto current_settings = context->getSettingsRef();
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                                current_settings).getSaturated(
                                    current_settings.max_execution_time);

            bool is_local = shard_info.isLocal();
            if (is_local)
                local_host = shard_info.local_addresses.begin()->readableString();

            auto connections = shard_info.pool->getMany(timeouts, &current_settings, PoolMode::GET_MANY);

            for (auto connection : connections)
            {
                if (std::count(fragments_distributed.data_to.begin(), fragments_distributed.data_to.end(), connection->getDescription()))
                {
                    LOG_DEBUG(&Poco::Logger::get("FragmentMgr"), "Fragment {} will send data to {}", fragments_distributed.fragment->getFragmentId(), connection->getDescription());
                    channels.emplace_back(DataSink::Channel{.connection = connection, .is_local = is_local});
                    break;
                }
            }
        }

        /// for exchange node
        for (const auto & node : fragments_distributed.fragment->getNodes())
        {
            auto it = fragments_distributed.data_from.find(node.plan_id);
            if (it != fragments_distributed.data_from.end())
            {
                if (auto * exchange_step = dynamic_cast<ExchangeDataStep *>(node.step.get()))
                {
                    exchange_step->setSources(it->second);
                }
                else
                    throw;
            }
        }

        if (local_host.empty())
            LOG_WARNING(&Poco::Logger::get("FragmentMgr"), "not found local_host from this fragment");

        QueryPipeline && pipeline = fragments_distributed.fragment->buildQueryPipeline(channels, local_host);

        // register ExchangeDataReceiver
        for (const auto & processor : pipeline.getProcessors())
        {
            if (ExchangeDataReceiver * receiver = dynamic_cast<ExchangeDataReceiver *>(processor.get()))
            {
                // register ExchangeDataReceiver
                const auto & receiver_key = FragmentDistributed::receiverKey(receiver->getPlanId(), receiver->getSource());
                fragments_distributed.receivers.emplace(receiver_key, receiver->shared_from_this());
            }
        }

        data->query_pipelines.emplace_back(std::move(pipeline));
    }

}

void FragmentMgr::executeQueryPipelines(const String & query_id)
{
    /// begin execute pipeline
    auto data = find(query_id);

    std::lock_guard lock(data->mutex);
    for (size_t i = 0; i < data->fragments_distributed.size(); ++i)
    {
        /// root fragment has't DestFragment, it's execute from tcphandler
        auto & fragment = data->fragments_distributed[i].fragment;
        if (fragment->getDestFragment())
        {
            onFinishCallBack call_back = [this, query_id = query_id, fragment_id = fragment->getFragmentId()]()
            {
                onFinish(query_id, fragment_id);
            };

            LOG_DEBUG(&Poco::Logger::get("FragmentMgr"), "Fragment {} begin execute", fragment->getFragmentId());
            executors.execute(data->query_pipelines[i], call_back);
        }
    }
}

QueryPipeline FragmentMgr::findRootQueryPipeline(const String & query_id)
{
    auto data = find(query_id);

    std::lock_guard lock(data->mutex);
    for (size_t i = 0; i < data->fragments_distributed.size(); ++i)
    {
        if (!data->fragments_distributed[i].fragment->getDestFragment())
        {
            return std::move(data->query_pipelines[i]);
        }
    }

    throw;
}

void FragmentMgr::rootQueryPipelineFinish(const String & query_id)
{
    auto data = find(query_id);

    FragmentID root_fragment_id = -1;
    {
        std::lock_guard lock(data->mutex);
        for (auto & fragment_distributed : data->fragments_distributed)
        {
            if (!fragment_distributed.fragment->getDestFragment())
            {
                root_fragment_id = fragment_distributed.fragment->getFragmentId();
            }
        }
    }

    if (root_fragment_id != -1)
        onFinish(query_id, root_fragment_id);
}

std::shared_ptr<ExchangeDataReceiver> FragmentMgr::findReceiver(const ExchangeDataRequest & exchange_data_request) const
{
    std::shared_ptr<ExchangeDataReceiver> receiver;
    {
        auto data = find(exchange_data_request.query_id);

        std::lock_guard lock(data->mutex);
        for (auto & fragment : data->fragments_distributed)
        {
            if (fragment.fragment->getFragmentId() == exchange_data_request.fragment_id)
            {
                const auto & receiver_key = FragmentDistributed::receiverKey(exchange_data_request.exchange_id, exchange_data_request.from_host);
                auto receiver_it = fragment.receivers.find(receiver_key);
                if (receiver_it == fragment.receivers.end())
                    throw;

                receiver = receiver_it->second;
                break;
            }
        }
    }

    if (!receiver)
        throw;

    return receiver;
}

void FragmentMgr::onFinish(const String & query_id, FragmentID fragment_id)
{
    LOG_DEBUG(&Poco::Logger::get("FragmentMgr"), "Query {} fragment {} execute finished", query_id, fragment_id);
    auto data = find(query_id);

    bool all_finished = true;
    {
        std::lock_guard lock(data->mutex);

        for (auto & fragment : data->fragments_distributed)
        {
            if (fragment.fragment->getFragmentId() == fragment_id)
            {
                fragment.is_finished = true;
            }
            else
            {
                if (!fragment.is_finished)
                {
                    all_finished = false;
                }
            }
        }
    }

    if (all_finished)
    {
        LOG_DEBUG(&Poco::Logger::get("FragmentMgr"), "Query {} fragment all finished", query_id);
        std::lock_guard lock(fragments_mutex);
        query_fragment.erase(query_id);
    }
}

}
