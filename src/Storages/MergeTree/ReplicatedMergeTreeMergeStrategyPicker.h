#pragma once

#include <common/types.h>
#include <optional>
#include <mutex>
#include <vector>
#include <atomic>
#include <boost/noncopyable.hpp>

namespace DB
{

class StorageReplicatedMergeTree;
struct ReplicatedMergeTreeLogEntryData;

/// In some use cases merging can be more expensive than fetching
/// (so instead of doing exactly the same merge cluster-wise you can do merge once and fetch ready part)
/// Fetches may be desirable for other operational reasons (backup replica without lot of CPU resources).
///
/// That class allow to take a decisions about preferred strategy for a concreate merge.
///
/// Since that code is used in shouldExecuteLogEntry we need to be able to:
/// 1) make decision fast
/// 2) avoid excessive zookeeper operations
///
/// Because of that we need to cache some important things,
/// like list of active replicas (to limit the number of zookeeper operations)
///
/// That means we need to refresh the state of that object regularly
///
/// NOTE: This class currently supports only single feature (execute_merges_on_single_replica_time_threshold),
/// may be extended to postpone merges in some other scenarios, namely
/// * always_fetch_merged_part
/// * try_fetch_recompressed_part_timeout
/// * (maybe, not for postpone) prefer_fetch_merged_part_time_threshold
///
/// NOTE: execute_merges_on_single_replica_time_threshold feature doesn't provide any strict guarantees.
/// When some replicas are added / removed we may execute some merges on more than one replica,
/// or not execute merge on any of replicas during execute_merges_on_single_replica_time_threshold interval.
/// (so it may be a bad idea to set that threshold to high values).
///
class ReplicatedMergeTreeMergeStrategyPicker: public boost::noncopyable
{
public:
    ReplicatedMergeTreeMergeStrategyPicker(StorageReplicatedMergeTree & storage_);

    /// triggers refreshing the cached state (list of replicas etc.)
    /// used when we get new merge event from the zookeeper queue ( see queueUpdatingTask() etc )
    void refreshState();

    /// return true if execute_merges_on_single_replica_time_threshold feature is active
    /// and we may need to do a fetch (or postpone) instead of merge
    bool shouldMergeOnSingleReplica(const ReplicatedMergeTreeLogEntryData & entry) const;

    /// return true if remote_fs_execute_merges_on_single_replica_time_threshold feature is active
    /// and we may need to do a fetch (or postpone) instead of merge
    bool shouldMergeOnSingleReplicaS3Shared(const ReplicatedMergeTreeLogEntryData & entry) const;

    /// returns the replica name
    /// and it's not current replica should do the merge
    /// used in shouldExecuteLogEntry and in tryExecuteMerge
    std::optional<String> pickReplicaToExecuteMerge(const ReplicatedMergeTreeLogEntryData & entry);

    /// checks (in zookeeper) if the picked replica finished the merge
    bool isMergeFinishedByReplica(const String & replica, const ReplicatedMergeTreeLogEntryData & entry);

private:
    StorageReplicatedMergeTree & storage;

    /// calculate entry hash based on zookeeper path and new part name
    /// ATTENTION: it's not a general-purpose hash, it just allows to select replicas consistently
    uint64_t getEntryHash(const ReplicatedMergeTreeLogEntryData & entry) const;

    std::atomic<time_t> execute_merges_on_single_replica_time_threshold = 0;
    std::atomic<time_t> remote_fs_execute_merges_on_single_replica_time_threshold = 0;
    std::atomic<time_t> last_refresh_time = 0;

    std::mutex mutex;

    /// those 2 members accessed under the mutex, only when
    /// execute_merges_on_single_replica_time_threshold enabled
    int current_replica_index = -1;
    std::vector<String> active_replicas;

};

}
