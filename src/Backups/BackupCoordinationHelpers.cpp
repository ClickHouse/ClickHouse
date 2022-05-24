#include <Backups/BackupCoordinationHelpers.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>
#include <base/chrono_io.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_BACKUP_TABLE;
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
    extern const int LOGICAL_ERROR;
}


struct BackupCoordinationReplicatedTablesInfo::HostAndTableName
{
    String host_id;
    DatabaseAndTableName table_name;

    struct Less
    {
        bool operator()(const HostAndTableName & lhs, const HostAndTableName & rhs) const
        {
            return (lhs.host_id < rhs.host_id) || ((lhs.host_id == rhs.host_id) && (lhs.table_name < rhs.table_name));
        }

        bool operator()(const std::shared_ptr<const HostAndTableName> & lhs, const std::shared_ptr<const HostAndTableName> & rhs) const
        {
            return operator()(*lhs, *rhs);
        }
    };
};


class BackupCoordinationReplicatedTablesInfo::CoveredPartsFinder
{
public:
    CoveredPartsFinder() = default;

    void addPart(const String & new_part_name, const std::shared_ptr<const HostAndTableName> & host_and_table_name)
    {
        addPart(MergeTreePartInfo::fromPartName(new_part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING), host_and_table_name);
    }

    void addPart(MergeTreePartInfo && new_part_info, const std::shared_ptr<const HostAndTableName> & host_and_table_name)
    {
        auto new_min_block = new_part_info.min_block;
        auto new_max_block = new_part_info.max_block;
        auto & parts = partitions[new_part_info.partition_id];

        /// Find the first part with max_block >= `part_info.min_block`.
        auto first_it = parts.lower_bound(new_min_block);
        if (first_it == parts.end())
        {
            /// All max_blocks < part_info.min_block, so we can safely add the `part_info` to the list of parts.
            parts.emplace(new_max_block, PartInfo{std::move(new_part_info), host_and_table_name});
            return;
        }

        {
            /// part_info.min_block <= current_info.max_block
            const auto & part = first_it->second;
            if (new_max_block < part.info.min_block)
            {
                /// (prev_info.max_block < part_info.min_block) AND (part_info.max_block < current_info.min_block),
                /// so we can safely add the `part_info` to the list of parts.
                parts.emplace(new_max_block, PartInfo{std::move(new_part_info), host_and_table_name});
                return;
            }

            /// (part_info.min_block <= current_info.max_block) AND (part_info.max_block >= current_info.min_block), parts intersect.

            if (part.info.contains(new_part_info))
            {
                /// `part_info` is already contained in another part.
                return;
            }
        }

        /// Probably `part_info` is going to replace multiple parts, find the range of parts to replace.
        auto last_it = first_it;
        while (last_it != parts.end())
        {
            const auto & part = last_it->second;
            if (part.info.min_block > new_max_block)
                break;
            if (!new_part_info.contains(part.info))
            {
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Intersected parts detected: {} in the table {}.{}{} and {} in the table {}.{}{}. It should be investigated",
                    part.info.getPartName(),
                    part.host_and_table_name->table_name.first,
                    part.host_and_table_name->table_name.second,
                    part.host_and_table_name->host_id.empty() ? "" : (" on the host " + part.host_and_table_name->host_id),
                    new_part_info.getPartName(),
                    host_and_table_name->table_name.first,
                    host_and_table_name->table_name.second,
                    host_and_table_name->host_id.empty() ? "" : (" on the host " + host_and_table_name->host_id));
            }
            ++last_it;
        }

        /// `part_info` will replace multiple parts [first_it..last_it)
        parts.erase(first_it, last_it);
        parts.emplace(new_max_block, PartInfo{std::move(new_part_info), host_and_table_name});
    }

    bool isCoveredByAnotherPart(const String & part_name) const
    {
        return isCoveredByAnotherPart(MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING));
    }

    bool isCoveredByAnotherPart(const MergeTreePartInfo & part_info) const
    {
        auto partition_it = partitions.find(part_info.partition_id);
        if (partition_it == partitions.end())
            return false;

        const auto & parts = partition_it->second;

        /// Find the first part with max_block >= `part_info.min_block`.
        auto it_part = parts.lower_bound(part_info.min_block);
        if (it_part == parts.end())
        {
            /// All max_blocks < part_info.min_block, so there is no parts covering `part_info`.
            return false;
        }

        /// part_info.min_block <= current_info.max_block
        const auto & existing_part = it_part->second;
        if (part_info.max_block < existing_part.info.min_block)
        {
            /// (prev_info.max_block < part_info.min_block) AND (part_info.max_block < current_info.min_block),
            /// so there is no parts covering `part_info`.
            return false;
        }

        /// (part_info.min_block <= current_info.max_block) AND (part_info.max_block >= current_info.min_block), parts intersect.

        if (existing_part.info == part_info)
        {
            /// It's the same part, it's kind of covers itself, but we check in this function whether a part is covered by another part.
            return false;
        }

        /// Check if `part_info` is covered by `current_info`.
        return existing_part.info.contains(part_info);
    }

private:
    struct PartInfo
    {
        MergeTreePartInfo info;
        std::shared_ptr<const HostAndTableName> host_and_table_name;
    };

    using Parts = std::map<Int64 /* max_block */, PartInfo>;
    std::unordered_map<String, Parts> partitions;
};


void BackupCoordinationReplicatedTablesInfo::addDataPath(const String & table_zk_path, const String & table_data_path)
{
    tables[table_zk_path].data_paths.push_back(table_data_path);
}

Strings BackupCoordinationReplicatedTablesInfo::getDataPaths(const String & table_zk_path) const
{
    auto it = tables.find(table_zk_path);
    if (it == tables.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getDataPaths() called for unknown table_zk_path: {}", table_zk_path);
    const auto & replicated_table = it->second;
    return replicated_table.data_paths;
}

void BackupCoordinationReplicatedTablesInfo::addPartNames(
    const String & host_id,
    const DatabaseAndTableName & table_name,
    const String & table_zk_path,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    auto & table = tables[table_zk_path];
    auto & part_locations_by_names = table.part_locations_by_names;
    auto host_and_table_name = std::make_shared<HostAndTableName>();
    host_and_table_name->host_id = host_id;
    host_and_table_name->table_name = table_name;

    for (const auto & part_name_and_checksum : part_names_and_checksums)
    {
        const auto & part_name = part_name_and_checksum.part_name;
        const auto & checksum = part_name_and_checksum.checksum;
        auto it = part_locations_by_names.find(part_name);
        if (it == part_locations_by_names.end())
        {
            it = part_locations_by_names.emplace(part_name, PartLocations{}).first;
            it->second.checksum = checksum;
        }
        else
        {
            const auto & existing = it->second;
            if (existing.checksum != checksum)
            {
                const auto & existing_host_and_table_name = **existing.host_and_table_names.begin();
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Table {}.{} has part {} which is different from the part of table {}.{}. Must be the same",
                    table_name.first,
                    table_name.second,
                    part_name,
                    existing_host_and_table_name.table_name.first,
                    existing_host_and_table_name.table_name.second);
            }
        }

        auto & host_and_table_names = it->second.host_and_table_names;

        /// `host_and_table_names` should be ordered because we need this vector to be in the same order on every replica.
        host_and_table_names.insert(
            std::upper_bound(host_and_table_names.begin(), host_and_table_names.end(), host_and_table_name, HostAndTableName::Less{}),
            host_and_table_name);
    }
}

Strings BackupCoordinationReplicatedTablesInfo::getPartNames(const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path) const
{
    if (!part_names_by_locations_prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "preparePartNamesByLocations() was not called before getPartNames()");

    auto it = tables.find(table_zk_path);
    if (it == tables.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getPartNames() called for unknown table_zk_path: {}", table_zk_path);
    const auto & table = it->second;
    auto it2 = table.part_names_by_locations.find(host_id);
    if (it2 == table.part_names_by_locations.end())
        return {};
    const auto & part_names_by_host_id = it2->second;
    auto it3 = part_names_by_host_id.find(table_name);
    if (it3 == part_names_by_host_id.end())
        return {};
    return it3->second;
}

void BackupCoordinationReplicatedTablesInfo::preparePartNamesByLocations()
{
    if (part_names_by_locations_prepared)
        return;
    part_names_by_locations_prepared = true;

    size_t counter = 0;
    for (auto & table : tables | boost::adaptors::map_values)
    {
        CoveredPartsFinder covered_parts_finder;
        for (const auto & [part_name, part_locations] : table.part_locations_by_names)
            covered_parts_finder.addPart(part_name, *part_locations.host_and_table_names.begin());

        table.part_names_by_locations.clear();
        for (const auto & [part_name, part_locations] : table.part_locations_by_names)
        {
            if (covered_parts_finder.isCoveredByAnotherPart(part_name))
                continue;
            size_t chosen_index = (counter++) % part_locations.host_and_table_names.size();
            const auto & chosen_host_id = part_locations.host_and_table_names[chosen_index]->host_id;
            const auto & chosen_table_name = part_locations.host_and_table_names[chosen_index]->table_name;
            table.part_names_by_locations[chosen_host_id][chosen_table_name].push_back(part_name);
        }
    }
}


BackupCoordinationDistributedBarrier::BackupCoordinationDistributedBarrier(
    const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, const String & logger_name_, const String & operation_name_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , log(&Poco::Logger::get(logger_name_))
    , operation_name(operation_name_)
{
    createRootNodes();
}

void BackupCoordinationDistributedBarrier::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
}

void BackupCoordinationDistributedBarrier::finish(const String & host_id, const String & error_message)
{
    if (error_message.empty())
        LOG_TRACE(log, "Host {} has finished {}", host_id, operation_name);
    else
        LOG_ERROR(log, "Host {} has failed {} with message: {}", host_id, operation_name, error_message);

    auto zookeeper = get_zookeeper();
    if (error_message.empty())
        zookeeper->create(zookeeper_path + "/" + host_id + ":ready", "", zkutil::CreateMode::Persistent);
    else
        zookeeper->create(zookeeper_path + "/" + host_id + ":error", error_message, zkutil::CreateMode::Persistent);
}

void BackupCoordinationDistributedBarrier::waitForAllHostsToFinish(const Strings & host_ids, const std::chrono::seconds timeout) const
{
    auto zookeeper = get_zookeeper();

    bool all_hosts_ready = false;
    String not_ready_host_id;
    String error_host_id;
    String error_message;

    /// Returns true of everything's ready, or false if we need to wait more.
    auto process_nodes = [&](const Strings & nodes)
    {
        std::unordered_set<std::string_view> set{nodes.begin(), nodes.end()};
        for (const String & host_id : host_ids)
        {
            if (set.contains(host_id + ":error"))
            {
                error_host_id = host_id;
                error_message = zookeeper->get(zookeeper_path + "/" + host_id + ":error");
                return;
            }
            if (!set.contains(host_id + ":ready"))
            {
                LOG_TRACE(log, "Waiting for host {} {}", host_id, operation_name);
                not_ready_host_id = host_id;
                return;
            }
        }

        all_hosts_ready = true;
    };

    std::atomic<bool> watch_set = false;
    std::condition_variable watch_triggered_event;

    auto watch_callback = [&](const Coordination::WatchResponse &)
    {
        watch_set = false; /// After it's triggered it's not set until we call getChildrenWatch() again.
        watch_triggered_event.notify_all();
    };

    auto watch_triggered = [&] { return !watch_set; };

    bool use_timeout = (timeout.count() >= 0);
    std::chrono::steady_clock::duration time_left = timeout;
    std::mutex dummy_mutex;

    while (true)
    {
        if (use_timeout && (time_left.count() <= 0))
        {
            Strings children = zookeeper->getChildren(zookeeper_path);
            process_nodes(children);
            break;
        }

        watch_set = true;
        Strings children = zookeeper->getChildrenWatch(zookeeper_path, nullptr, watch_callback);
        process_nodes(children);

        if (!error_message.empty() || all_hosts_ready)
            break;

        {
            std::unique_lock dummy_lock{dummy_mutex};
            if (use_timeout)
            {
                std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
                if (!watch_triggered_event.wait_for(dummy_lock, time_left, watch_triggered))
                    break;
                time_left -= (std::chrono::steady_clock::now() - start_time);
            }
            else
                watch_triggered_event.wait(dummy_lock, watch_triggered);
        }
    }

    if (watch_set)
    {
        /// Remove watch by triggering it.
        zookeeper->create(zookeeper_path + "/remove_watch-", "", zkutil::CreateMode::EphemeralSequential);
        std::unique_lock dummy_lock{dummy_mutex};
        watch_triggered_event.wait_for(dummy_lock, timeout, watch_triggered);
    }

    if (!error_message.empty())
    {
        throw Exception(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Host {} failed {} with message: {}",
            error_host_id,
            operation_name,
            error_message);
    }

    if (all_hosts_ready)
    {
        LOG_TRACE(log, "All hosts have finished {}", operation_name);
        return;
    }


    throw Exception(
        ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
        "Host {} has failed {}: Time ({}) is out",
        not_ready_host_id,
        operation_name,
        to_string(timeout));
}

}
