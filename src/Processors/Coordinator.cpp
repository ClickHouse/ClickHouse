
#include <Processors/Coordinator.h>
#include <Processors/QueryPlan/ScanStep.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

void Coordinator::schedule()
{
    // If the fragment has a scanstep, it is scheduled according to the cluster copy fragment

    std::unordered_map<UInt32, std::vector<String>> scan_fragment_hosts;
    PoolBase<DB::Connection>::Entry local_shard_connection;
    for (UInt32 i = 0; i < fragments.size(); ++i)
    {
        auto fragment = fragments[i];
        auto lowest_node = fragment->getQueryPlan().getNodes().begin();

        if (auto scan_step = dynamic_cast<ScanStep *>(lowest_node->step.get()))
        {
            for (const auto & shard_info : fragment->getCluster()->getShardsInfo())
            {
                auto current_settings = context->getSettingsRef();
                auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                                    current_settings).getSaturated(
                                        current_settings.max_execution_time);
                std::vector<ConnectionPoolWithFailover::TryResult> try_results;

                try_results = shard_info.pool->getManyChecked(timeouts, &current_settings, PoolMode::GET_MANY, scan_step->getTable()->getStorageID().getQualifiedName());

                if (shard_info.isLocal())
                    local_shard_connection = try_results[0].entry;

                PoolBase<DB::Connection>::Entry connection =  try_results[0].entry; /// TODO random ?
                host_connection[connection->getDescription()] = connection;

                scan_fragment_hosts[i].emplace_back(connection->getDescription());
                fragment_id_hosts[i].emplace_back(connection->getDescription());

                host_fragment_ids[connection->getDescription()].emplace_back(fragment);
            }
        }
    }

    // For a fragment with a scanstep, process its dest fragment.

    auto process_other_fragment = [this, &local_shard_connection](std::unordered_map<UInt32, std::vector<String>> & fragment_hosts) -> std::unordered_map<UInt32, std::vector<String>>
    {
        std::unordered_map<UInt32, std::vector<String>> local_fragment_hosts;
        for (const auto & [fragment_id, hosts] : fragment_hosts)
        {
            auto dest_fragment = fragments[fragment_id]->getDestFragment();

            if (!dest_fragment)
                return local_fragment_hosts;

            if (fragment_id_hosts.contains(dest_fragment->getFragmentId()))
                return local_fragment_hosts;

            if (!dest_fragment->isPartitioned())
            {
                host_fragment_ids[local_shard_connection->getDescription()].emplace_back(dest_fragment);
                fragment_id_hosts[dest_fragment->getFragmentId()].emplace_back(local_shard_connection->getDescription());
                local_fragment_hosts[dest_fragment->getFragmentId()].emplace_back(local_shard_connection->getDescription());

                continue;
            }

            for (auto host : hosts)
            {
                host_fragment_ids[host].emplace_back(dest_fragment);
                fragment_id_hosts[dest_fragment->getFragmentId()].emplace_back(host);
                local_fragment_hosts[dest_fragment->getFragmentId()].emplace_back(host);
            }
        }
        return local_fragment_hosts;
    };

    std::optional<std::unordered_map<UInt32, std::vector<String>>> fragment_hosts(scan_fragment_hosts);
    while (!fragment_hosts->empty())
    {
        std::unordered_map<UInt32, std::vector<String>> local_fragment_hosts = process_other_fragment(fragment_hosts.value());
        fragment_hosts->swap(local_fragment_hosts);
    }

    // dump
    for (auto [host, fragments_for_dump] : host_fragment_ids)
    {
        for (auto fragment : fragments_for_dump)
        {
            LOG_INFO(&Poco::Logger::get("Coordinator"), "host {}, fragment_id {}", host, std::to_string(fragment->getFragmentId()));
        }
    }

}

}
