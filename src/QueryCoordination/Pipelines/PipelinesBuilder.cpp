#include <QueryCoordination/Pipelines/PipelinesBuilder.h>
#include <QueryCoordination/Exchange/ExchangeDataSink.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryCoordination/Exchange/ExchangeManager.h>
#include <Interpreters/Cluster.h>


namespace DB
{

Pipelines PipelinesBuilder::build()
{
    Pipelines pipelines;
    for (DistributedFragment & distributed_fragment : distributed_fragments)
    {
        auto fragment = distributed_fragment.getFragment();
        const auto & data_to = distributed_fragment.getDataTo();
        for (const auto & to : data_to)
        {
            LOG_DEBUG(log, "Fragment {} will send data to {}", fragment->getFragmentID(), to);
        }

        /// for data sink
        std::vector<ExchangeDataSink::Channel> channels;
        String local_host; /// for DataSink, we need tell peer who am i.

        auto all_addresses = cluster->getShardsAddresses();
        auto & shards_info = cluster->getShardsInfo();

        for (size_t i = 0; i < all_addresses.size(); i++)
        {
            const auto & shard_nodes = all_addresses[i];
            /// find target host_port for this shard
            String target_host_port;
            for (const auto & address : shard_nodes)
            {
                if (address.is_local)
                    local_host = address.toString();

                if (std::count(data_to.begin(), data_to.end(), address.toString()))
                {
                    target_host_port = address.toString();
                    break;
                }
            }

            if (target_host_port.empty())
                continue;

            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings).getSaturated(settings.max_execution_time);
            auto connection = shards_info[i].pool->getOne(timeouts, &settings, target_host_port);

            LOG_DEBUG(log, "Fragment {} will actually send data to {}", fragment->getFragmentID(), connection->getDescription());
            channels.emplace_back(ExchangeDataSink::Channel{.connection = connection, .is_local = (local_host == target_host_port)});
        }

        /// for exchange node
        for (const auto & node : fragment->getNodes())
        {
            const auto & data_from = distributed_fragment.getDataFrom();
            auto it = data_from.find(node.plan_id);
            if (it != data_from.end())
            {
                if (auto * exchange_step = dynamic_cast<ExchangeDataStep *>(node.step.get()))
                {
                    exchange_step->setSources(it->second);
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Destination step {} is not ExchangeDataStep", node.plan_id);
            }
        }

        if (local_host.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found my host and port in fragment clusters");

        QueryPipeline && pipeline = fragment->buildQueryPipeline(channels, local_host);

        // register ExchangeDataSource
        for (const auto & processor : pipeline.getProcessors())
        {
            if (ExchangeDataSource * receiver = dynamic_cast<ExchangeDataSource *>(processor.get()))
            {
                // register ExchangeDataSource
                ExchangeDataRequest request{
                    .from_host = receiver->getSource(),
                    .query_id = query_id,
                    .fragment_id = fragment->getFragmentID(),
                    .exchange_id = receiver->getPlanId()};

                ExchangeManager::getInstance().registerExchangeDataSource(request, receiver->shared_from_this());
            }
        }

        if (!fragment->hasDestFragment())
        {
            pipelines.addRootPipeline(fragment->getFragmentID(), std::move(pipeline));
        }
        else
        {
            pipelines.addSourcesPipeline(fragment->getFragmentID(), std::move(pipeline));
        }
    }

    pipelines.assignThreadNum(settings.max_threads);

    return pipelines;
}

}
