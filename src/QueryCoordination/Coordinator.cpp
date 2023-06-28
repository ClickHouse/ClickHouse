#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryCoordination/Coordinator.h>
#include <Storages/IStorage.h>
#include <QueryCoordination/FragmentMgr.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

void Coordinator::scheduleExecuteDistributedPlan()
{
    // If the fragment has a scanstep, it is scheduled according to the cluster copy fragment
    const String & local_host = assignFragmentToHost();

    for (auto & [host, fragment_ids] : host_fragments)
    {
        for (auto & fragment : fragment_ids)
        {
            LOG_INFO(&Poco::Logger::get("Coordinator"), "host_fragment_ids: host {}, fragment {}", host, fragment->getFragmentId());
        }
    }

    for (auto & [fragment_id, hosts] : fragment_hosts)
    {
        for (auto & host : hosts)
        {
            LOG_INFO(&Poco::Logger::get("Coordinator"), "fragment_id_hosts: host {}, fragment {}", host, fragment_id);
        }
    }

    sendFragmentToDistributed(local_host);

    sendExecuteQueryPipelines(local_host);
}

String Coordinator::assignFragmentToHost()
{
    std::unordered_map<FragmentID, std::vector<String>> scan_fragment_hosts;
    String local_host_port;
    for (const auto & fragment : fragments)
    {
        auto fragment_id = fragment->getFragmentId();
        id_fragment[fragment_id] = fragment;

        for (const auto & node : fragment->getNodes())
        {
            if (auto * read_step = dynamic_cast<ReadFromMergeTree *>(node.step.get()))
            {
                for (const auto & shard_info : fragment->getCluster()->getShardsInfo())
                {
                    const auto & table_name = read_step->getStorageSnapshot()->storage.getStorageID().getQualifiedName();

                    String host_port;
                    bool need_get_connect = true;
                    if (shard_info.isLocal())
                    {
                        local_host_port = shard_info.local_addresses[0].toString();

                        if (isUpToDate(table_name))
                        {
                            host_port = shard_info.local_addresses[0].toString();
                            need_get_connect = false;
                        }
                        else
                        {
                            need_get_connect = true;
                        }
                    }


                    PoolBase<DB::Connection>::Entry connection;
                    if (need_get_connect)
                    {
                        auto current_settings = context->getSettingsRef();
                        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                                            current_settings).getSaturated(
                                                current_settings.max_execution_time);
                        std::vector<ConnectionPoolWithFailover::TryResult> try_results
                            = shard_info.pool->getManyChecked(timeouts, &current_settings, PoolMode::GET_MANY, table_name);
                        connection =  try_results[0].entry; /// TODO random ?
                        host_port = connection->getDescription();
                    }

                    host_connection[host_port] = connection;

                    scan_fragment_hosts[fragment_id].emplace_back(host_port);
                    fragment_hosts[fragment_id].emplace_back(host_port);
                    host_fragments[host_port].emplace_back(fragment);
                }
            }
        }
    }

    // For a fragment with a scanstep, process its dest fragment.

    auto process_other_fragment
        = [this, &local_host_port](
              std::unordered_map<FragmentID, std::vector<String>> & fragment_hosts_) -> std::unordered_map<FragmentID, std::vector<String>>
    {
        std::unordered_map<FragmentID, std::vector<String>> this_fragment_hosts;
        for (const auto & [fragment_id, hosts] : fragment_hosts_)
        {
            auto dest_fragment = fragments[fragment_id]->getDestFragment();

            if (!dest_fragment)
                return this_fragment_hosts;

            /// dest_fragment scheduling by the left node
            if (dest_fragment->getChildren().size() > 1)
            {
                if (fragment_id != dest_fragment->getChildren()[0]->getFragmentId())
                    continue;
            }

            if (fragment_hosts_.contains(dest_fragment->getFragmentId()))
                return this_fragment_hosts;

            if (!dest_fragment->isPartitioned())
            {
                if (!dest_fragment->getDestFragment()) /// root fragment
                {
                    host_fragments[local_host_port].emplace_back(dest_fragment);
                    fragment_hosts[dest_fragment->getFragmentId()].emplace_back(local_host_port);
                    this_fragment_hosts[dest_fragment->getFragmentId()].emplace_back(local_host_port);
                }
                else
                {
                    const auto & host = hosts[0];
                    host_fragments[host].emplace_back(dest_fragment);
                    fragment_hosts[dest_fragment->getFragmentId()].emplace_back(host);
                    this_fragment_hosts[dest_fragment->getFragmentId()].emplace_back(host);
                }

                continue;
            }

            for (const auto & host : hosts)
            {
                auto & dest_hosts = fragment_hosts[dest_fragment->getFragmentId()];
                if (!std::count(dest_hosts.begin(), dest_hosts.end(), host))
                {
                    host_fragments[host].emplace_back(dest_fragment);
                    dest_hosts.emplace_back(host);
                    this_fragment_hosts[dest_fragment->getFragmentId()].emplace_back(host);
                }
            }
        }
        return this_fragment_hosts;
    };

    std::optional<std::unordered_map<FragmentID, std::vector<String>>> fragment_hosts_(scan_fragment_hosts);
    while (!fragment_hosts_->empty())
    {
        std::unordered_map<FragmentID, std::vector<String>> tmp_fragment_hosts = process_other_fragment(fragment_hosts_.value());
        fragment_hosts_->swap(tmp_fragment_hosts);
    }

    return local_host_port;
}

bool Coordinator::isUpToDate(const QualifiedTableName & table_name)
{
    auto context_to_resolve_table_names = context;
    auto resolved_id = context_to_resolve_table_names->tryResolveStorageID({table_name.database, table_name.table});
    StoragePtr table = DatabaseCatalog::instance().tryGetTable(resolved_id, context_to_resolve_table_names);
    if (!table)
        return false;

    TableStatus status;
    if (auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        status.is_replicated = true;
        status.absolute_delay = static_cast<UInt32>(replicated_table->getAbsoluteDelay());
    }
    else
        status.is_replicated = false;

    bool is_up_to_date = false;
    UInt64 max_allowed_delay = UInt64(context->getSettingsRef().max_replica_delay_for_distributed_queries);
    if (!max_allowed_delay)
    {
        is_up_to_date = true;
        return is_up_to_date;
    }

    UInt32 delay = status.absolute_delay;

    if (delay < max_allowed_delay)
        is_up_to_date = true;
    else
    {
        is_up_to_date = false;
    }
    return is_up_to_date;
}

std::unordered_map<FragmentID, FragmentRequest> Coordinator::buildFragmentRequest()
{
    std::unordered_map<FragmentID, FragmentRequest> fragment_requests;

    /// assign fragment id
    for (auto & [fragment_id, _] : fragment_hosts)
    {
        auto & request = fragment_requests[fragment_id];
        request.fragment_id = fragment_id;
    }

    /// assign data to and data from
    for (auto & [fragment_id, hosts] : fragment_hosts)
    {
        auto & request = fragment_requests[fragment_id];

        auto fragment = id_fragment[fragment_id];
        auto dest_fragment = fragment->getDestFragment();
        auto dest_exchange_id = fragment->getDestExchangeID();

        Destinations data_to;
        if (dest_fragment)
        {
            auto dest_fragment_id = dest_fragment->getFragmentId();
            data_to = fragment_hosts[dest_fragment_id];

            /// dest_fragment exchange data_from is current fragment hosts
            auto & dest_request = fragment_requests[dest_fragment_id];
            auto & exchange_data_from = dest_request.data_from[dest_exchange_id];
            exchange_data_from.insert(exchange_data_from.begin(), hosts.begin(), hosts.end());
        }

        request.data_to = data_to;
    }

    return fragment_requests;
}


void Coordinator::sendFragmentToDistributed(const String & local_shard_host)
{
    const std::unordered_map<FragmentID, FragmentRequest> & fragment_requests = buildFragmentRequest();

    for (const auto & [f_id, request] : fragment_requests)
    {
        LOG_INFO(&Poco::Logger::get("Coordinator"), "Fragment id {}, request {}", f_id, request.toString());
    }

    // send
    for (auto [host, fragments_for_send] : host_fragments)
    {
        FragmentsRequest fragments_request;
        for (const auto & fragment : fragments_for_send)
        {
            const auto & [_, request] = *fragment_requests.find(fragment->getFragmentId());
            fragments_request.fragments_request.emplace_back(request);
            if (host == local_shard_host)
            {
                FragmentMgr::getInstance().addFragment(context->getCurrentQueryId(), fragment, context);
            }
        }

        if (host == local_shard_host)
        {
            FragmentMgr::getInstance().fragmentsToDistributed(context->getCurrentQueryId(), fragments_request.fragments_request);
        }
        else
        {
            auto current_settings = context->getSettingsRef();
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                                current_settings).getSaturated(
                                    current_settings.max_execution_time);

            ClientInfo modified_client_info = context->getClientInfo();
            modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

            host_connection[host]->sendFragments(
                timeouts,
                query,
                context->getQueryParameters(),
                context->getCurrentQueryId(),
                QueryProcessingStage::Complete,
                &context->getSettingsRef(),
                &modified_client_info,
                fragments_request);
        }
    }

    // receive ready
    for (auto & [host, _] : host_fragments)
    {
        if (host != local_shard_host)
        {
            auto package = host_connection[host]->receivePacket();

            size_t max_try_num = 5;
            size_t try_num = 0;
            while (package.type != Protocol::Server::FragmentsReady)
            {
                if (try_num >= max_try_num)
                {
                    break;
                }
                package = host_connection[host]->receivePacket();
                try_num++;
            }

            if (package.type != Protocol::Server::FragmentsReady)
                throw;
        }
    }
}

void Coordinator::sendExecuteQueryPipelines(const String & local_shard_host)
{
    for (auto [host, fragments_for_dump] : host_fragments)
    {
        if (host == local_shard_host)
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
