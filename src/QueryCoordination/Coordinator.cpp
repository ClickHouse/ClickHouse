#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryCoordination/Coordinator.h>
#include <Storages/IStorage.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <QueryCoordination/Fragments/DistributedFragmentBuilder.h>
#include <QueryCoordination/Pipelines/PipelinesBuilder.h>
#include <QueryCoordination/fragmentsToPipelines.h>
#include <QueryCoordination/QueryCoordinationMetaInfo.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

Coordinator::Coordinator(const FragmentPtrs & fragments_, ContextMutablePtr context_, String query_)
    : log(&Poco::Logger::get("Coordinator"))
    , fragments(fragments_)
    , context(context_)
    , query(query_)
{
    for (const auto & fragment : fragments)
    {
        auto fragment_id = fragment->getFragmentID();
        id_fragment[fragment_id] = fragment;
    }
}

void Coordinator::schedulePrepareDistributedPipelines()
{
    // If the fragment has a scanstep, it is scheduled according to the cluster copy fragment
    assignFragmentToHost();

    for (auto & [host, fragment_ids] : host_fragments)
    {
        for (auto & fragment : fragment_ids)
        {
            LOG_INFO(log, "host_fragment_ids: host {}, fragment {}", host, fragment->getFragmentID());
        }
    }

    for (auto & [fragment_id, hosts] : fragment_hosts)
    {
        for (auto & host : hosts)
        {
            LOG_INFO(log, "fragment_id_hosts: host {}, fragment {}", host, fragment_id);
        }
    }

    sendFragmentsToPreparePipelines();

    sendBeginExecutePipelines();
}

PoolBase<DB::Connection>::Entry Coordinator::getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name)
{
    auto current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                        current_settings).getSaturated(
                            current_settings.max_execution_time);
    auto try_results = shard_info.pool->getManyChecked(timeouts, &current_settings, PoolMode::GET_ONE, table_name);
    return try_results[0].entry;
}

PoolBase<DB::Connection>::Entry Coordinator::getConnection(const Cluster::ShardInfo & shard_info)
{
    auto current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                        current_settings).getSaturated(
                            current_settings.max_execution_time);
    auto try_results = shard_info.pool->getMany(timeouts, &current_settings, PoolMode::GET_ONE);
    return try_results[0];
}

std::unordered_map<UInt32, std::vector<String>> Coordinator::assignSourceFragment()
{
    std::unordered_map<UInt32, std::vector<String>> scan_fragment_hosts;
    for (const auto & fragment : fragments)
    {
        auto fragment_id = fragment->getFragmentID();
        for (const auto & node : fragment->getNodes())
        {
            if (node.step->stepType() != StepType::Scan)
                continue;

            auto cluster = context->getClusters().find(context->getQueryCoordinationMetaInfo().cluster_name)->second;
            for (const auto & shard_info : cluster->getShardsInfo())
            {
                PoolBase<DB::Connection>::Entry connection;
                String host_port;
                if (shard_info.isLocal())
                {
                    local_host = shard_info.local_addresses[0].toString();
                    host_port = local_host;

                    if (auto * read_step = typeid_cast<ReadFromMergeTree *>(node.step.get()))
                    {
                        const auto & table_name = read_step->getStorageSnapshot()->storage.getStorageID().getQualifiedName();
                        if (!isUpToDate(table_name))
                        {
                            connection =  getConnection(shard_info, table_name);
                            host_port = connection->getDescription();
                        }
                    }
                }
                else
                {
                    connection =  getConnection(shard_info);
                    host_port = connection->getDescription();
                }

                host_connection[host_port] = connection;

                scan_fragment_hosts[fragment_id].emplace_back(host_port);
                fragment_hosts[fragment_id].emplace_back(host_port);
                host_fragments[host_port].emplace_back(fragment);
            }
        }
    }

    return scan_fragment_hosts;
}

void Coordinator::assignFragmentToHost()
{
    const std::unordered_map<UInt32, std::vector<String>> & scan_fragment_hosts = assignSourceFragment();

    // For a fragment with a scanstep, process its dest fragment.

    auto process_other_fragment
        = [this](
              std::unordered_map<UInt32, std::vector<String>> & fragment_hosts_) -> std::unordered_map<UInt32, std::vector<String>>
    {
        std::unordered_map<UInt32, std::vector<String>> this_fragment_hosts;
        for (const auto & [fragment_id, hosts] : fragment_hosts_)
        {
            auto dest_fragment_id = id_fragment[fragment_id]->getDestFragmentID();

            if (!id_fragment[fragment_id]->hasDestFragment())
                return this_fragment_hosts;

            auto dest_fragment = id_fragment[dest_fragment_id];

            /// dest_fragment scheduling by the left node
            if (dest_fragment->getChildren().size() > 1)
            {
                if (fragment_id != dest_fragment->getChildren()[0]->getFragmentID())
                    continue;
            }

            if (fragment_hosts_.contains(dest_fragment->getFragmentID()))
                return this_fragment_hosts;

            if (typeid_cast<ExchangeDataStep *>(id_fragment[fragment_id]->getDestExchangeNode()->step.get())->isSingleton())
            {
                if (!dest_fragment->hasDestFragment()) /// root fragment
                {
                    host_fragments[local_host].emplace_back(dest_fragment);
                    fragment_hosts[dest_fragment->getFragmentID()].emplace_back(local_host);
                    this_fragment_hosts[dest_fragment->getFragmentID()].emplace_back(local_host);
                }
                else
                {
                    const auto & host = hosts[0];
                    host_fragments[host].emplace_back(dest_fragment);
                    fragment_hosts[dest_fragment->getFragmentID()].emplace_back(host);
                    this_fragment_hosts[dest_fragment->getFragmentID()].emplace_back(host);
                }

                continue;
            }

            for (const auto & host : hosts)
            {
                auto & dest_hosts = fragment_hosts[dest_fragment->getFragmentID()];
                if (!std::count(dest_hosts.begin(), dest_hosts.end(), host))
                {
                    host_fragments[host].emplace_back(dest_fragment);
                    dest_hosts.emplace_back(host);
                    this_fragment_hosts[dest_fragment->getFragmentID()].emplace_back(host);
                }
            }
        }
        return this_fragment_hosts;
    };

    std::optional<std::unordered_map<UInt32, std::vector<String>>> fragment_hosts_(scan_fragment_hosts);
    while (!fragment_hosts_->empty())
    {
        std::unordered_map<UInt32, std::vector<String>> tmp_fragment_hosts = process_other_fragment(fragment_hosts_.value());
        fragment_hosts_->swap(tmp_fragment_hosts);
    }
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

std::unordered_map<UInt32, FragmentRequest> Coordinator::buildFragmentRequest()
{
    std::unordered_map<UInt32, FragmentRequest> fragment_requests;

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
        auto dest_fragment = id_fragment[fragment->getDestFragmentID()];

        Destinations data_to;
        if (dest_fragment)
        {
            auto dest_exchange_id = fragment->getDestExchangeID();
            auto dest_fragment_id = dest_fragment->getFragmentID();
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


void Coordinator::sendFragmentsToPreparePipelines()
{
    const std::unordered_map<UInt32, FragmentRequest> & fragment_requests = buildFragmentRequest();

    for (const auto & [f_id, request] : fragment_requests)
    {
        LOG_INFO(log, "Send fragment to distributed request {}", request.toString());
    }

    auto current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                        current_settings).getSaturated(
                            current_settings.max_execution_time);

    ClientInfo modified_client_info = context->getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    // send
    for (auto [host, fragments_for_send] : host_fragments)
    {
        FragmentsRequest fragments_request;
        for (const auto & fragment : fragments_for_send)
        {
            const auto & [_, request] = *fragment_requests.find(fragment->getFragmentID());
            fragments_request.fragments_request.emplace_back(request);
        }

        if (host == local_host)
        {
            /// local direct to pipelines
            auto cluster = context->getClusters().find(context->getQueryCoordinationMetaInfo().cluster_name)->second;
            pipelines = fragmentsToPipelines(fragments, fragments_request.fragmentsRequest(), context->getCurrentQueryId(), context->getSettingsRef(), cluster);
        }
        else
        {
            host_connection[host]->sendFragments(
                timeouts,
                query,
                context->getQueryParameters(),
                context->getCurrentQueryId(),
                QueryProcessingStage::Complete,
                &context->getSettingsRef(),
                &modified_client_info,
                fragments_request,
                context->getQueryCoordinationMetaInfo());
        }
    }

    // receive ready
    for (auto & [host, _] : host_fragments)
    {
        if (host != local_host)
        {
            auto package = host_connection[host]->receivePacket();

            if (package.type == Protocol::Server::Exception)
                package.exception->rethrow();

            if (package.type != Protocol::Server::PipelinesReady)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No receive ready from {}", host);
        }
    }
}

void Coordinator::sendBeginExecutePipelines()
{
    for (auto [host, _] : host_fragments)
    {
        if (host != local_host)
        {
            host_connection[host]->sendBeginExecutePipelines(context->getCurrentQueryId());
        }
    }
}

std::unordered_map<String, IConnectionPool::Entry> Coordinator::getRemoteHostConnection()
{
    std::unordered_map<String, IConnectionPool::Entry> res;
    for (auto [host, _] : host_fragments)
    {
        if (host != local_host)
        {
            res.emplace(host, host_connection[host]);
        }
    }
    return res;
}

}
