#include <Backups/BackupCoordinationHelpers.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
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


namespace
{
    struct LessReplicaName
    {
        bool operator()(const std::shared_ptr<const String> & left, const std::shared_ptr<const String> & right) { return *left < *right; }
    };
}

using MutationInfo = IBackupCoordination::MutationInfo;


class BackupCoordinationReplicatedPartsAndMutations::CoveredPartsFinder
{
public:
    explicit CoveredPartsFinder(const String & table_name_for_logs_) : table_name_for_logs(table_name_for_logs_) {}

    void addPartInfo(MergeTreePartInfo && new_part_info, const std::shared_ptr<const String> & replica_name)
    {
        auto new_min_block = new_part_info.min_block;
        auto new_max_block = new_part_info.max_block;
        auto & parts = partitions[new_part_info.partition_id];

        /// Find the first part with max_block >= `part_info.min_block`.
        auto first_it = parts.lower_bound(new_min_block);
        if (first_it == parts.end())
        {
            /// All max_blocks < part_info.min_block, so we can safely add the `part_info` to the list of parts.
            parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_name});
            return;
        }

        {
            /// part_info.min_block <= current_info.max_block
            const auto & part = first_it->second;
            if (new_max_block < part.info.min_block)
            {
                /// (prev_info.max_block < part_info.min_block) AND (part_info.max_block < current_info.min_block),
                /// so we can safely add the `part_info` to the list of parts.
                parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_name});
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
                    "Intersected parts detected: {} on replica {} and {} on replica {}",
                    part.info.getPartName(),
                    *part.replica_name,
                    new_part_info.getPartName(),
                    *replica_name);
            }
            ++last_it;
        }

        /// `part_info` will replace multiple parts [first_it..last_it)
        parts.erase(first_it, last_it);
        parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_name});
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
        std::shared_ptr<const String> replica_name;
    };

    using Parts = std::map<Int64 /* max_block */, PartInfo>;
    std::unordered_map<String, Parts> partitions;
    const String table_name_for_logs;
};


BackupCoordinationReplicatedPartsAndMutations::BackupCoordinationReplicatedPartsAndMutations() = default;
BackupCoordinationReplicatedPartsAndMutations::~BackupCoordinationReplicatedPartsAndMutations() = default;

void BackupCoordinationReplicatedPartsAndMutations::addPartNames(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addPartNames() must not be called after preparing");

    auto & table_info = table_infos[table_shared_id];
    table_info.table_name_for_logs = table_name_for_logs;

    if (!table_info.covered_parts_finder)
        table_info.covered_parts_finder = std::make_unique<CoveredPartsFinder>(table_name_for_logs);

    auto replica_name_ptr = std::make_shared<String>(replica_name);

    for (const auto & part_name_and_checksum : part_names_and_checksums)
    {
        const auto & part_name = part_name_and_checksum.part_name;
        const auto & checksum = part_name_and_checksum.checksum;
        auto it = table_info.replicas_by_part_name.find(part_name);
        if (it == table_info.replicas_by_part_name.end())
        {
            it = table_info.replicas_by_part_name.emplace(part_name, PartReplicas{}).first;
            it->second.checksum = checksum;
        }
        else
        {
            const auto & other = it->second;
            if (other.checksum != checksum)
            {
                const String & other_replica_name = **other.replica_names.begin();
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Table {} on replica {} has part {} which is different from the part on replica {}. Must be the same",
                    table_name_for_logs,
                    replica_name,
                    part_name,
                    other_replica_name);
            }
        }

        auto & replica_names = it->second.replica_names;

        /// `replica_names` should be ordered because we need this vector to be in the same order on every replica.
        replica_names.insert(
            std::upper_bound(replica_names.begin(), replica_names.end(), replica_name_ptr, LessReplicaName{}), replica_name_ptr);
    }
}

Strings BackupCoordinationReplicatedPartsAndMutations::getPartNames(const String & table_shared_id, const String & replica_name) const
{
    prepare();
    
    auto it = table_infos.find(table_shared_id);
    if (it == table_infos.end())
        return {};
    
    const auto & part_names_by_replica_name = it->second.part_names_by_replica_name;
    auto it2 = part_names_by_replica_name.find(replica_name);
    if (it2 == part_names_by_replica_name.end())
        return {};
    
    return it2->second;
}

void BackupCoordinationReplicatedPartsAndMutations::addMutations(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<MutationInfo> & mutations)
{
    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addMutations() must not be called after preparing");

    auto & table_info = table_infos[table_shared_id];
    table_info.table_name_for_logs = table_name_for_logs;
    for (const auto & [mutation_id, mutation_entry] : mutations)
        table_info.mutations.emplace(mutation_id, mutation_entry);

    /// std::max() because the calculation must give the same result being repeated on a different replica.
    table_info.replica_name_to_store_mutations = std::max(table_info.replica_name_to_store_mutations, replica_name);
}

std::vector<MutationInfo>
BackupCoordinationReplicatedPartsAndMutations::getMutations(const String & table_shared_id, const String & replica_name) const
{
    prepare();
    
    auto it = table_infos.find(table_shared_id);
    if (it == table_infos.end())
        return {};
    
    const auto & table_info = it->second;
    if (table_info.replica_name_to_store_mutations != replica_name)
        return {};

    std::vector<MutationInfo> res;
    for (const auto & [mutation_id, mutation_entry] : table_info.mutations)
        res.emplace_back(MutationInfo{mutation_id, mutation_entry});
    return res;
}

void BackupCoordinationReplicatedPartsAndMutations::prepare() const
{
    if (prepared)
        return;

    size_t counter = 0;
    for (const auto & table_info : table_infos | boost::adaptors::map_values)
    {
        try
        {
            /// Remove parts covered by other parts.
            for (const auto & [part_name, part_replicas] : table_info.replicas_by_part_name)
            {
                auto part_info = MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
                
                auto & min_data_versions_by_partition = table_info.min_data_versions_by_partition;
                auto it2 = min_data_versions_by_partition.find(part_info.partition_id);
                if (it2 == min_data_versions_by_partition.end())
                    min_data_versions_by_partition[part_info.partition_id] = part_info.getDataVersion();
                else
                    it2->second = std::min(it2->second, part_info.getDataVersion());

                table_info.covered_parts_finder->addPartInfo(std::move(part_info), part_replicas.replica_names[0]);
            }

            for (const auto & [part_name, part_replicas] : table_info.replicas_by_part_name)
            {
                if (table_info.covered_parts_finder->isCoveredByAnotherPart(part_name))
                    continue;
                size_t chosen_index = (counter++) % part_replicas.replica_names.size();
                const auto & chosen_replica_name = *part_replicas.replica_names[chosen_index];
                table_info.part_names_by_replica_name[chosen_replica_name].push_back(part_name);
            }

            /// Remove finished or unrelated mutations.
            std::unordered_map<String, String> unfinished_mutations;
            for (const auto & [mutation_id, mutation_entry_str] : table_info.mutations)
            {
                auto mutation_entry = ReplicatedMergeTreeMutationEntry::parse(mutation_entry_str, mutation_id);
                std::map<String, Int64> new_block_numbers;
                for (const auto & [partition_id, block_number] : mutation_entry.block_numbers)
                {
                    auto it = table_info.min_data_versions_by_partition.find(partition_id);
                    if ((it != table_info.min_data_versions_by_partition.end()) && (it->second < block_number))
                        new_block_numbers[partition_id] = block_number;
                }
                mutation_entry.block_numbers = std::move(new_block_numbers);
                if (!mutation_entry.block_numbers.empty())
                    unfinished_mutations[mutation_id] = mutation_entry.toString();
            }
            table_info.mutations = unfinished_mutations;
        }
        catch (Exception & e)
        {
            e.addMessage("While checking data of table {}", table_info.table_name_for_logs);
            throw;
        }
    }

    prepared = true;
}


/// Helper designed to be used in an implementation of the IBackupCoordination interface in the part related to replicated access storages.
BackupCoordinationReplicatedAccess::BackupCoordinationReplicatedAccess() = default;
BackupCoordinationReplicatedAccess::~BackupCoordinationReplicatedAccess() = default;

void BackupCoordinationReplicatedAccess::addFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path)
{
    auto & ref = file_paths_by_zk_path[std::make_pair(access_zk_path, access_entity_type)];
    ref.file_paths.emplace(file_path);

    /// std::max() because the calculation must give the same result being repeated on a different replica.
    ref.host_to_store_access = std::max(ref.host_to_store_access, host_id);
}

Strings BackupCoordinationReplicatedAccess::getFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const
{
    auto it = file_paths_by_zk_path.find(std::make_pair(access_zk_path, access_entity_type));
    if (it == file_paths_by_zk_path.end())
        return {};

    auto & file_paths = it->second;
    if (file_paths.host_to_store_access != host_id)
        return {};

    Strings res{file_paths.file_paths.begin(), file_paths.file_paths.end()};
    return res;
}


/// Helps to wait until all hosts come to a specified stage.
BackupCoordinationStatusSync::BackupCoordinationStatusSync(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, Poco::Logger * log_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , log(log_)
{
    createRootNodes();
}

void BackupCoordinationStatusSync::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
}

void BackupCoordinationStatusSync::set(const String & current_host, const String & new_status, const String & message)
{
    setImpl(current_host, new_status, message, {}, {});
}

Strings BackupCoordinationStatusSync::setAndWait(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts)
{
    return setImpl(current_host, new_status, message, all_hosts, {});
}

Strings BackupCoordinationStatusSync::setAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, UInt64 timeout_ms)
{
    return setImpl(current_host, new_status, message, all_hosts, timeout_ms);
}

Strings BackupCoordinationStatusSync::setImpl(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, const std::optional<UInt64> & timeout_ms)
{
    /// Put new status to ZooKeeper.
    auto zookeeper = get_zookeeper();
    zookeeper->createIfNotExists(zookeeper_path + "/" + current_host + "|" + new_status, message);

    if (all_hosts.empty() || (new_status == kErrorStatus))
        return {};

    if ((all_hosts.size() == 1) && (all_hosts.front() == current_host))
        return {message};

    /// Wait for other hosts.

    Strings ready_hosts_results;
    ready_hosts_results.resize(all_hosts.size());

    std::map<String, std::vector<size_t> /* index in `ready_hosts_results` */> unready_hosts;
    for (size_t i = 0; i != all_hosts.size(); ++i)
        unready_hosts[all_hosts[i]].push_back(i);

    std::optional<String> host_with_error;
    std::optional<String> error_message;

    /// Process ZooKeeper's nodes and set `all_hosts_ready` or `unready_host` or `error_message`.
    auto process_zk_nodes = [&](const Strings & zk_nodes)
    {
        for (const String & zk_node : zk_nodes)
        {
            if (zk_node.starts_with("remove_watch-"))
                continue;

            size_t separator_pos = zk_node.find('|');
            if (separator_pos == String::npos)
                throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Unexpected zk node {}", zookeeper_path + "/" + zk_node);
            String host = zk_node.substr(0, separator_pos);
            String status = zk_node.substr(separator_pos + 1);
            if (status == kErrorStatus)
            {
                host_with_error = host;
                error_message = zookeeper->get(zookeeper_path + "/" + zk_node);
                return;
            }
            auto it = unready_hosts.find(host);
            if ((it != unready_hosts.end()) && (status == new_status))
            {
                String result = zookeeper->get(zookeeper_path + "/" + zk_node);
                for (size_t i : it->second)
                    ready_hosts_results[i] = result;
                unready_hosts.erase(it);
            }
        }
    };

    /// Wait until all hosts are ready or an error happens or time is out.
    std::atomic<bool> watch_set = false;
    std::condition_variable watch_triggered_event;

    auto watch_callback = [&](const Coordination::WatchResponse &)
    {
        watch_set = false; /// After it's triggered it's not set until we call getChildrenWatch() again.
        watch_triggered_event.notify_all();
    };

    auto watch_triggered = [&] { return !watch_set; };

    bool use_timeout = timeout_ms.has_value();
    std::chrono::milliseconds timeout{timeout_ms.value_or(0)};
    std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
    std::chrono::steady_clock::duration elapsed;
    std::mutex dummy_mutex;

    while (!unready_hosts.empty() && !error_message)
    {
        watch_set = true;
        Strings nodes = zookeeper->getChildrenWatch(zookeeper_path, nullptr, watch_callback);
        process_zk_nodes(nodes);

        if (!unready_hosts.empty() && !error_message)
        {
            LOG_TRACE(log, "Waiting for host {}", unready_hosts.begin()->first);
            std::unique_lock dummy_lock{dummy_mutex};
            if (use_timeout)
            {
                elapsed = std::chrono::steady_clock::now() - start_time;
                if ((elapsed > timeout) || !watch_triggered_event.wait_for(dummy_lock, timeout - elapsed, watch_triggered))
                    break;
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
        watch_triggered_event.wait(dummy_lock, watch_triggered);
    }

    if (error_message)
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Error occurred on host {}: {}", *host_with_error, *error_message);

    if (!unready_hosts.empty())
    {
        throw Exception(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Waited for host {} too long ({})",
            unready_hosts.begin()->first,
            to_string(elapsed));
    }

    return ready_hosts_results;
}

}
