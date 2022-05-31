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


namespace
{
    struct LessReplicaName
    {
        bool operator()(const std::shared_ptr<const String> & left, const std::shared_ptr<const String> & right) { return *left < *right; }
    };
}


class BackupCoordinationReplicatedPartNames::CoveredPartsFinder
{
public:
    CoveredPartsFinder(const String & table_name_for_logs_) : table_name_for_logs(table_name_for_logs_) {}

    void addPartName(const String & new_part_name, const std::shared_ptr<const String> & replica_name)
    {
        addPartName(MergeTreePartInfo::fromPartName(new_part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING), replica_name);
    }

    void addPartName(MergeTreePartInfo && new_part_info, const std::shared_ptr<const String> & replica_name)
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
                    "Intersected parts detected in the table {}: {} on replica {} and {} on replica {}. It should be investigated",
                    table_name_for_logs,
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


BackupCoordinationReplicatedPartNames::BackupCoordinationReplicatedPartNames() = default;
BackupCoordinationReplicatedPartNames::~BackupCoordinationReplicatedPartNames() = default;

void BackupCoordinationReplicatedPartNames::addPartNames(
    const String & table_zk_path,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    if (part_names_prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addPartNames() must not be called after getPartNames()");

    auto & table_info = table_infos[table_zk_path];
    if (!table_info.covered_parts_finder)
        table_info.covered_parts_finder = std::make_unique<CoveredPartsFinder>(table_name_for_logs);

    auto replica_name_ptr = std::make_shared<String>(replica_name);

    for (const auto & part_name_and_checksum : part_names_and_checksums)
    {
        const auto & part_name = part_name_and_checksum.part_name;
        const auto & checksum = part_name_and_checksum.checksum;
        auto it = table_info.parts_replicas.find(part_name);
        if (it == table_info.parts_replicas.end())
        {
            it = table_info.parts_replicas.emplace(part_name, PartReplicas{}).first;
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

        table_info.covered_parts_finder->addPartName(part_name, replica_name_ptr);
    }
}

Strings BackupCoordinationReplicatedPartNames::getPartNames(const String & table_zk_path, const String & replica_name) const
{
    preparePartNames();
    auto it = table_infos.find(table_zk_path);
    if (it == table_infos.end())
        return {};
    const auto & replicas_parts = it->second.replicas_parts;
    auto it2 = replicas_parts.find(replica_name);
    if (it2 == replicas_parts.end())
        return {};
    return it2->second;
}

void BackupCoordinationReplicatedPartNames::preparePartNames() const
{
    if (part_names_prepared)
        return;

    size_t counter = 0;
    for (const auto & table_info : table_infos | boost::adaptors::map_values)
    {
        for (const auto & [part_name, part_replicas] : table_info.parts_replicas)
        {
            if (table_info.covered_parts_finder->isCoveredByAnotherPart(part_name))
                continue;
            size_t chosen_index = (counter++) % part_replicas.replica_names.size();
            const auto & chosen_replica_name = *part_replicas.replica_names[chosen_index];
            table_info.replicas_parts[chosen_replica_name].push_back(part_name);
        }
    }

    part_names_prepared = true;
}


/// Helps to wait until all hosts come to a specified stage.
BackupCoordinationStageSync::BackupCoordinationStageSync(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, Poco::Logger * log_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , log(log_)
{
    createRootNodes();
}

void BackupCoordinationStageSync::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
}

void BackupCoordinationStageSync::syncStage(const String & current_host, int new_stage, const Strings & wait_hosts, std::chrono::seconds timeout)
{
   /// Put new stage to ZooKeeper.
    auto zookeeper = get_zookeeper();
    zookeeper->createIfNotExists(zookeeper_path + "/" + current_host + "|" + std::to_string(new_stage), "");

    if (wait_hosts.empty() || ((wait_hosts.size() == 1) && (wait_hosts.front() == current_host)))
        return;

    /// Wait for other hosts.

    /// Current stages of all hosts.
    std::optional<String> host_with_error;
    std::optional<String> error_message;

    std::map<String, std::optional<int>> unready_hosts;
    for (const String & host : wait_hosts)
        unready_hosts.emplace(host, std::optional<int>{});

    /// Process ZooKeeper's nodes and set `all_hosts_ready` or `unready_host` or `error_message`.
    auto process_zk_nodes = [&](const Strings & zk_nodes)
    {
        for (const String & zk_node : zk_nodes)
        {
            if (zk_node == "error")
            {
                String str = zookeeper->get(zookeeper_path + "/" + zk_node);
                size_t separator_pos = str.find('|');
                if (separator_pos == String::npos)
                    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Unexpected value of zk node {}: {}", zookeeper_path + "/" + zk_node, str);
                host_with_error = str.substr(0, separator_pos);
                error_message = str.substr(separator_pos + 1);
                return;
            }
            else if (!zk_node.starts_with("remove_watch-"))
            {
                size_t separator_pos = zk_node.find('|');
                if (separator_pos == String::npos)
                    throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Unexpected zk node {}", zookeeper_path + "/" + zk_node);
                String host = zk_node.substr(0, separator_pos);
                int found_stage = parseFromString<int>(zk_node.substr(separator_pos + 1));
                auto it = unready_hosts.find(host);
                if (it != unready_hosts.end())
                {
                    auto & stage = it->second;
                    if (!stage || (stage < found_stage))
                        stage = found_stage;
                    if (stage >= new_stage)
                        unready_hosts.erase(it);
                }
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

    bool use_timeout = (timeout.count() >= 0);
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
}

void BackupCoordinationStageSync::syncStageError(const String & current_host, const String & error_message)
{
    auto zookeeper = get_zookeeper();
    zookeeper->createIfNotExists(zookeeper_path + "/error", current_host + "|" + error_message);
}

}
