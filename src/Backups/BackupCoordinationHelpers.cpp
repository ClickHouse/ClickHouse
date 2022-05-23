#include <Backups/BackupCoordinationHelpers.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <IO/ReadHelpers.h>
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


BackupCoordinationHostIDAndStorageID::BackupCoordinationHostIDAndStorageID(const String & host_id_, const StorageID & storage_id_)
    : host_id(host_id_), storage_id(storage_id_)
{
}

String BackupCoordinationHostIDAndStorageID::serialize() const
{
    return serialize(host_id, storage_id);
}

String BackupCoordinationHostIDAndStorageID::serialize(const String & host_id_, const StorageID & storage_id_)
{
    return host_id_ + "|" + escapeForFileName(storage_id_.database_name) + "|"
        + escapeForFileName(storage_id_.table_name) + "|" + toString(storage_id_.uuid);
}

BackupCoordinationHostIDAndStorageID BackupCoordinationHostIDAndStorageID::deserialize(const String & str)
{
    size_t separator_pos = str.find('|');
    if (separator_pos == String::npos)
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Couldn't deserialize host id and storage id");
    String host_id = str.substr(0, separator_pos);
    size_t prev_separator_pos = separator_pos;
    separator_pos = str.find('|', prev_separator_pos + 1);
    if (separator_pos == String::npos)
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Couldn't deserialize host id and storage id");
    String database_name = unescapeForFileName(str.substr(prev_separator_pos + 1, separator_pos - prev_separator_pos - 1));
    prev_separator_pos = separator_pos;
    separator_pos = str.find('|', prev_separator_pos + 1);
    if (separator_pos == String::npos)
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Couldn't deserialize host id and storage id");
    String table_name = unescapeForFileName(str.substr(prev_separator_pos + 1, separator_pos - prev_separator_pos - 1));
    UUID uuid = parse<UUID>(str.substr(separator_pos + 1));
    return BackupCoordinationHostIDAndStorageID{host_id, StorageID{database_name, table_name, uuid}};
}

bool BackupCoordinationHostIDAndStorageID::Less::operator()(const BackupCoordinationHostIDAndStorageID & lhs, const BackupCoordinationHostIDAndStorageID & rhs) const
{
    return (lhs.host_id < rhs.host_id) || ((lhs.host_id == rhs.host_id) && (lhs.storage_id < rhs.storage_id));
}

bool BackupCoordinationHostIDAndStorageID::Less::operator()(const std::shared_ptr<const BackupCoordinationHostIDAndStorageID> & lhs, const std::shared_ptr<const BackupCoordinationHostIDAndStorageID> & rhs) const
{
    return operator()(*lhs, *rhs);
}


class BackupCoordinationReplicatedPartNames::CoveredPartsFinder
{
public:
    CoveredPartsFinder() = default;

    void addPart(const String & new_part_name, const std::shared_ptr<const BackupCoordinationHostIDAndStorageID> & host_and_table_id)
    {
        addPart(MergeTreePartInfo::fromPartName(new_part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING), host_and_table_id);
    }

    void addPart(MergeTreePartInfo && new_part_info, const std::shared_ptr<const BackupCoordinationHostIDAndStorageID> & host_and_table_id)
    {
        auto new_min_block = new_part_info.min_block;
        auto new_max_block = new_part_info.max_block;
        auto & parts = partitions[new_part_info.partition_id];

        /// Find the first part with max_block >= `part_info.min_block`.
        auto first_it = parts.lower_bound(new_min_block);
        if (first_it == parts.end())
        {
            /// All max_blocks < part_info.min_block, so we can safely add the `part_info` to the list of parts.
            parts.emplace(new_max_block, PartInfo{std::move(new_part_info), host_and_table_id});
            return;
        }

        {
            /// part_info.min_block <= current_info.max_block
            const auto & part = first_it->second;
            if (new_max_block < part.info.min_block)
            {
                /// (prev_info.max_block < part_info.min_block) AND (part_info.max_block < current_info.min_block),
                /// so we can safely add the `part_info` to the list of parts.
                parts.emplace(new_max_block, PartInfo{std::move(new_part_info), host_and_table_id});
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
                    "Intersected parts detected: {} in the table {}{} and {} in the table {}{}. It should be investigated",
                    part.info.getPartName(),
                    part.host_and_table_id->storage_id.getNameForLogs(),
                    part.host_and_table_id->host_id.empty() ? "" : (" on the host " + part.host_and_table_id->host_id),
                    new_part_info.getPartName(),
                    host_and_table_id->storage_id.getNameForLogs(),
                    host_and_table_id->host_id.empty() ? "" : (" on the host " + host_and_table_id->host_id));
            }
            ++last_it;
        }

        /// `part_info` will replace multiple parts [first_it..last_it)
        parts.erase(first_it, last_it);
        parts.emplace(new_max_block, PartInfo{std::move(new_part_info), host_and_table_id});
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
        std::shared_ptr<const BackupCoordinationHostIDAndStorageID> host_and_table_id;
    };

    using Parts = std::map<Int64 /* max_block */, PartInfo>;
    std::unordered_map<String, Parts> partitions;
};


void BackupCoordinationReplicatedPartNames::addPartNames(
    const String & host_id,
    const StorageID & table_id,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums,
    const String & table_zk_path)
{
    auto & table_info = tables_by_zk_path[table_zk_path];
    tables[host_id][table_id].table_info = &table_info;
    auto & part_names_with_locations = table_info.part_names_with_locations;
    auto host_and_table_id = std::make_shared<BackupCoordinationHostIDAndStorageID>(host_id, table_id);

    for (const auto & part_name_and_checksum : part_names_and_checksums)
    {
        const auto & part_name = part_name_and_checksum.part_name;
        const auto & checksum = part_name_and_checksum.checksum;
        auto it = part_names_with_locations.find(part_name);
        if (it == part_names_with_locations.end())
        {
            it = part_names_with_locations.emplace(part_name, PartLocations{}).first;
            it->second.checksum = checksum;
        }
        else
        {
            const auto & existing = it->second;
            if (existing.checksum != checksum)
            {
                const auto & existing_host_and_table_id = **existing.hosts_and_tables.begin();
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Table {} has part {} which is different from the part of table {}. Must be the same",
                    table_id.getNameForLogs(),
                    part_name,
                    existing_host_and_table_id.storage_id.getNameForLogs());
            }
        }

        auto & host_and_table_names = it->second.hosts_and_tables;

        /// `host_and_table_names` should be ordered because we need this vector to be in the same order on every replica.
        host_and_table_names.insert(
            std::upper_bound(
                host_and_table_names.begin(), host_and_table_names.end(), host_and_table_id, BackupCoordinationHostIDAndStorageID::Less{}),
            host_and_table_id);
    }
}

bool BackupCoordinationReplicatedPartNames::has(const String & host_id, const StorageID & table_id) const
{
    auto it = tables.find(host_id);
    if (it == tables.end())
        return false;
    return it->second.contains(table_id);
}

const BackupCoordinationReplicatedPartNames::ExtendedTableInfo & BackupCoordinationReplicatedPartNames::getTableInfo(const String & host_id, const StorageID & table_id) const
{
    auto it = tables.find(host_id);
    if (it == tables.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getTableInfo() called for unknown table");
    auto it2 = it->second.find(table_id);
    if (it2 == it->second.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getTableInfo() called for unknown table");
    return it2->second;
}

BackupCoordinationReplicatedPartNames::ExtendedTableInfo & BackupCoordinationReplicatedPartNames::getTableInfo(const String & host_id, const StorageID & table_id)
{
    auto it = tables.find(host_id);
    if (it == tables.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getTableInfo() called for unknown table");
    auto it2 = it->second.find(table_id);
    if (it2 == it->second.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getTableInfo() called for unknown table");
    return it2->second;
}

void BackupCoordinationReplicatedPartNames::addDataPath(const String & host_id, const StorageID & table_id, const String & data_path)
{
    getTableInfo(host_id, table_id).table_info->data_paths.push_back(data_path);
}

Strings BackupCoordinationReplicatedPartNames::getDataPaths(const String & host_id, const StorageID & table_id) const
{
    return getTableInfo(host_id, table_id).table_info->data_paths;
}

Strings BackupCoordinationReplicatedPartNames::getPartNames(const String & host_id, const StorageID & table_id) const
{
    if (!part_names_prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "preparePartNamesByLocations() was not called before getPartNames()");
    return getTableInfo(host_id, table_id).part_names;
}

void BackupCoordinationReplicatedPartNames::preparePartNames()
{
    if (part_names_prepared)
        return;
    part_names_prepared = true;

    size_t counter = 0;
    for (auto & table : tables_by_zk_path | boost::adaptors::map_values)
    {
        CoveredPartsFinder covered_parts_finder;
        for (const auto & [part_name, part_locations] : table.part_names_with_locations)
            covered_parts_finder.addPart(part_name, *part_locations.hosts_and_tables.begin());

        for (const auto & [part_name, part_locations] : table.part_names_with_locations)
        {
            if (covered_parts_finder.isCoveredByAnotherPart(part_name))
                continue;
            size_t chosen_index = (counter++) % part_locations.hosts_and_tables.size();
            const auto & chosen_host_id = part_locations.hosts_and_tables[chosen_index]->host_id;
            const auto & chosen_table_id = part_locations.hosts_and_tables[chosen_index]->storage_id;
            getTableInfo(chosen_host_id, chosen_table_id).part_names.push_back(part_name);
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
