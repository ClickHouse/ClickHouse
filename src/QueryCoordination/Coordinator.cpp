
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryCoordination/Coordinator.h>
#include <Storages/IStorage.h>
#include <QueryCoordination/IO/FragmentsRequest.h>
#include <QueryCoordination/FragmentMgr.h>

namespace DB
{

void Coordinator::scheduleExecuteDistributedPlan()
{
    // If the fragment has a scanstep, it is scheduled according to the cluster copy fragment

    std::unordered_map<FragmentID, std::vector<String>> scan_fragment_hosts;
    PoolBase<DB::Connection>::Entry local_shard_connection;
    for (UInt32 i = 0; i < fragments.size(); ++i)
    {
        auto fragment = fragments[i];
        auto lowest_node = fragment->getQueryPlan().getNodes().begin();

        if (auto scan_step = dynamic_cast<ReadFromMergeTree *>(lowest_node->step.get()))
        {
            for (const auto & shard_info : fragment->getCluster()->getShardsInfo())
            {
                auto current_settings = context->getSettingsRef();
                auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                                    current_settings).getSaturated(
                                        current_settings.max_execution_time);
                std::vector<IConnectionPool::Entry> try_results;

//                try_results = shard_info.pool->getManyChecked(timeouts, &current_settings, PoolMode::GET_MANY, scan_step->getTable()->getStorageID().getQualifiedName());
                try_results = shard_info.pool->getMany(timeouts, &current_settings, PoolMode::GET_MANY);

                if (shard_info.isLocal())
                    local_shard_connection = try_results[0];

                PoolBase<DB::Connection>::Entry connection =  try_results[0]; /// TODO random ?
                host_connection[connection->getDescription()] = connection;

                scan_fragment_hosts[i].emplace_back(connection->getDescription());
                fragment_id_hosts[i].emplace_back(connection->getDescription());

                host_fragment_ids[connection->getDescription()].emplace_back(fragment);
            }
        }
    }

    // For a fragment with a scanstep, process its dest fragment.

    auto process_other_fragment
        = [this, &local_shard_connection](
              std::unordered_map<FragmentID, std::vector<String>> & fragment_hosts) -> std::unordered_map<FragmentID, std::vector<String>>
    {
        std::unordered_map<FragmentID, std::vector<String>> tmp_fragment_hosts;
        for (const auto & [fragment_id, hosts] : fragment_hosts)
        {
            auto dest_fragment = fragments[fragment_id]->getDestFragment();

            if (!dest_fragment)
                return tmp_fragment_hosts;

            if (fragment_id_hosts.contains(dest_fragment->getFragmentId()))
                return tmp_fragment_hosts;

            if (!dest_fragment->isPartitioned())
            {
                host_fragment_ids[local_shard_connection->getDescription()].emplace_back(dest_fragment);
                fragment_id_hosts[dest_fragment->getFragmentId()].emplace_back(local_shard_connection->getDescription());
                tmp_fragment_hosts[dest_fragment->getFragmentId()].emplace_back(local_shard_connection->getDescription());

                continue;
            }

            for (auto host : hosts)
            {
                host_fragment_ids[host].emplace_back(dest_fragment);
                fragment_id_hosts[dest_fragment->getFragmentId()].emplace_back(host);
                tmp_fragment_hosts[dest_fragment->getFragmentId()].emplace_back(host);
            }
        }
        return tmp_fragment_hosts;
    };

    std::optional<std::unordered_map<FragmentID, std::vector<String>>> fragment_hosts(scan_fragment_hosts);
    while (!fragment_hosts->empty())
    {
        std::unordered_map<FragmentID, std::vector<String>> tmp_fragment_hosts = process_other_fragment(fragment_hosts.value());
        fragment_hosts->swap(tmp_fragment_hosts);
    }

    sendFragmentToDistributed(local_shard_connection);

    sendExecuteQueryPipelines(local_shard_connection);
}


void Coordinator::sendFragmentToDistributed(const PoolBase<DB::Connection>::Entry & local_shard_connection)
{
    // send
    for (auto [host, fragments_for_send] : host_fragment_ids)
    {
        if (host == local_shard_connection->getDescription())
        {
            FragmentsRequest fragments_request;
            for (auto fragment : fragments_for_send)
            {
                LOG_INFO(&Poco::Logger::get("Coordinator"), "host {}, fragment_id {}", host, std::to_string(fragment->getFragmentId()));

                Destinations dest_hosts;
                if (fragment->getDestFragment())
                {
                    dest_hosts = std::move(fragment_id_hosts[fragment->getDestFragment()->getFragmentId()]);
                }
                fragments_request.fragments_request.emplace_back(FragmentRequest{.fragment_id = fragment->getFragmentId(), .destinations = std::move(dest_hosts)});

                FragmentMgr::getInstance().addFragment(context->getCurrentQueryId(), fragment, context);
            }
            FragmentMgr::getInstance().fragmentsToDistributed(context->getCurrentQueryId(), fragments_request.fragments_request);
        }
        else
        {
            FragmentsRequest request; // query_id fragment dests for host
            request.query = query;
            for (auto fragment : fragments_for_send)
            {
                LOG_INFO(&Poco::Logger::get("Coordinator"), "host {}, fragment_id {}", host, std::to_string(fragment->getFragmentId()));

                auto & dest_hosts = fragment_id_hosts[fragment->getDestFragment()->getFragmentId()];

                request.fragments_request.emplace_back(FragmentRequest{.fragment_id = fragment->getFragmentId(), .destinations = std::move(dest_hosts)});
            }

            auto current_settings = context->getSettingsRef();
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                                current_settings).getSaturated(
                                    current_settings.max_execution_time);

            host_connection[host]->sendFragments(timeouts, query, context->getQueryParameters(), context->getCurrentQueryId(), QueryProcessingStage::Complete, &context->getSettingsRef(),
                                                 &context->getClientInfo(), false, {}, request);
        }
    }

    // receive ready
    for (auto [host, fragments_for_send] : host_fragment_ids)
    {
        if (host != local_shard_connection->getDescription())
        {
            const auto & package = host_connection[host]->receivePacket();

            if (package.type != Protocol::Server::FragmentsReady)
                throw;
        }
    }
}

void Coordinator::sendExecuteQueryPipelines(const PoolBase<DB::Connection>::Entry & local_shard_connection)
{
    for (auto [host, fragments_for_dump] : host_fragment_ids)
    {
        if (host == local_shard_connection->getDescription())
        {
            FragmentMgr::getInstance().executeQueryPipelines(context->getCurrentQueryId());
        }
        else
        {
            host_connection[host]->sendExecuteQueryPipelines(context->getCurrentQueryId());
        }
    }
}

}
