#include <Storages/MergeTree/ReplicatedMergeTreeMergeStrategyPicker.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>

#include <base/types.h>
#include <base/sort.h>
#include <optional>
#include <mutex>
#include <city.h>
#include <algorithm>
#include <atomic>


namespace DB
{

/// don't refresh state too often (to limit number of zookeeper ops)
static const auto REFRESH_STATE_MINIMUM_INTERVAL_SECONDS = 3;

/// refresh the state automatically it it was not refreshed for a longer time
static const auto REFRESH_STATE_MAXIMUM_INTERVAL_SECONDS = 30;


ReplicatedMergeTreeMergeStrategyPicker::ReplicatedMergeTreeMergeStrategyPicker(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , parts_on_active_replicas(storage_.format_version)
{}


bool ReplicatedMergeTreeMergeStrategyPicker::isMergeMutationFinishedByAnyReplica(const ReplicatedMergeTreeLogEntryData & entry)
{
    std::lock_guard lock(mutex);
    return !parts_on_active_replicas.getContainingPart(entry.new_part_name).empty();
}


bool ReplicatedMergeTreeMergeStrategyPicker::shouldMergeMutateOnSingleReplica(const ReplicatedMergeTreeLogEntryData & entry) const
{
    time_t threshold = execute_merges_on_single_replica_time_threshold;
    return (
        threshold > 0       /// feature turned on
        && (entry.type == ReplicatedMergeTreeLogEntry::MERGE_PARTS || entry.type == ReplicatedMergeTreeLogEntry::MUTATE_PART)
        && entry.create_time + threshold > time(nullptr)          /// not too much time waited
    );
}


bool ReplicatedMergeTreeMergeStrategyPicker::shouldMergeMutateOnSingleReplicaShared(const ReplicatedMergeTreeLogEntryData & entry) const
{
    time_t threshold = remote_fs_execute_merges_on_single_replica_time_threshold;
    return (
        threshold > 0       /// feature turned on
        && (entry.type == ReplicatedMergeTreeLogEntry::MERGE_PARTS || entry.type == ReplicatedMergeTreeLogEntry::MUTATE_PART)
        && entry.create_time + threshold > time(nullptr)          /// not too much time waited
    );
}


/// that will return the same replica name for ReplicatedMergeTreeLogEntry on all the replicas (if the replica set is the same).
/// that way each replica knows who is responsible for doing a certain merge.

/// in some corner cases (added / removed / deactivated replica)
/// nodes can pick different replicas to execute merge and wait for it (or to execute the same merge together)
/// but that doesn't have a significant impact (in one case it will wait for the execute_merges_on_single_replica_time_threshold,
/// in another just 2 replicas will do the merge)
std::optional<String> ReplicatedMergeTreeMergeStrategyPicker::pickReplicaToExecuteMergeMutation(const ReplicatedMergeTreeLogEntryData & entry)
{
    /// last state refresh was too long ago, need to sync up the replicas list
    if (time(nullptr) - last_refresh_time > REFRESH_STATE_MAXIMUM_INTERVAL_SECONDS)
        refreshState();

    auto hash = getEntryHash(entry);

    std::lock_guard lock(mutex);

    auto num_replicas = active_replicas.size();

    if (num_replicas == 0)
        return std::nullopt;

    auto replica_index = static_cast<int>(hash % num_replicas);

    if (replica_index == current_replica_index)
        return std::nullopt;

    return active_replicas.at(replica_index);
}


void ReplicatedMergeTreeMergeStrategyPicker::refreshState()
{
    const auto settings = storage.getSettings();
    auto threshold = settings->execute_merges_on_single_replica_time_threshold.totalSeconds();
    auto threshold_init = 0;
    if (settings->allow_remote_fs_zero_copy_replication)
        threshold_init = settings->remote_fs_execute_merges_on_single_replica_time_threshold.totalSeconds();

    if (threshold == 0)
        /// we can reset the settings w/o lock (it's atomic)
        execute_merges_on_single_replica_time_threshold = threshold;
    if (threshold_init == 0)
        remote_fs_execute_merges_on_single_replica_time_threshold = threshold_init;
    if (threshold == 0 && threshold_init == 0)
        return;

    auto now = time(nullptr);

    /// the setting was already enabled, and last state refresh was done recently
    if (((threshold != 0 && execute_merges_on_single_replica_time_threshold != 0)
        || (threshold_init != 0 && remote_fs_execute_merges_on_single_replica_time_threshold != 0))
        && now - last_refresh_time < REFRESH_STATE_MINIMUM_INTERVAL_SECONDS)
        return;

    auto zookeeper = storage.getZooKeeper();
    auto all_replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas");

    ::sort(all_replicas.begin(), all_replicas.end());

    std::vector<String> active_replicas_tmp;
    int current_replica_index_tmp = -1;
    ActiveDataPartSet active_parts_tmp(storage.format_version);

    for (const String & replica : all_replicas)
    {
        auto replica_path = fs::path{storage.zookeeper_path} / "replicas" / replica;
        if (zookeeper->exists(replica_path / "is_active"))
        {
            Strings parts = zookeeper->getChildren(replica_path / "parts");
            for (const auto & part : parts)
                active_parts_tmp.add(part);

            active_replicas_tmp.push_back(replica);
            if (replica == storage.replica_name)
            {
                current_replica_index_tmp = active_replicas_tmp.size() - 1;
            }
        }
    }

    if (current_replica_index_tmp < 0 || active_replicas_tmp.size() < 2)
    {
        if (execute_merges_on_single_replica_time_threshold > 0)
        {
            LOG_WARNING(storage.log, "Can't find current replica in the active replicas list, or too few active replicas to use 'execute_merges_on_single_replica_time_threshold'");
            /// we can reset the settings w/o lock (it's atomic)
            execute_merges_on_single_replica_time_threshold = 0;
        }
        /// default value of remote_fs_execute_merges_on_single_replica_time_threshold is not 0
        /// so we write no warning in log here
        remote_fs_execute_merges_on_single_replica_time_threshold = 0;
        return;
    }

    std::lock_guard lock(mutex);
    if (threshold != 0) /// Zeros already reset
        execute_merges_on_single_replica_time_threshold = threshold;
    if (threshold_init != 0)
        remote_fs_execute_merges_on_single_replica_time_threshold = threshold_init;
    last_refresh_time = now;
    current_replica_index = current_replica_index_tmp;
    active_replicas = active_replicas_tmp;
    parts_on_active_replicas = active_parts_tmp;
}


uint64_t ReplicatedMergeTreeMergeStrategyPicker::getEntryHash(const ReplicatedMergeTreeLogEntryData & entry) const
{
    auto hash_data = storage.zookeeper_path + entry.new_part_name;
    return CityHash_v1_0_2::CityHash64(hash_data.c_str(), hash_data.length());
}


}
