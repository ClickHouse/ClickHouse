#pragma once

#include <common/types.h>
// #include <Common/ZooKeeper/ZooKeeper.h>
// #include <common/logger_useful.h>
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
class ReplicatedMergeTreeMergeStrategyPicker: public boost::noncopyable
{
public:
    ReplicatedMergeTreeMergeStrategyPicker(StorageReplicatedMergeTree & storage_);

    /// triggers refreshing the cached state (list of replicas etc.)
    /// used when we get new merge event from the zookeeper queue ( see queueUpdatingTask() etc )
    void refreshState();

    /// returns the replica name in the case when feature is active
    /// and it's not current replica should do the merge
    /// used in shouldExecuteLogEntry and in tryExecuteMerge
    std::optional<std::string> pickReplicaToExecuteMerge(const ReplicatedMergeTreeLogEntryData & entry);

    /// checks (in zookeeper) if the picked replica finished the merge
    bool isMergeFinishedByReplica(const String & replica, const ReplicatedMergeTreeLogEntryData & entry);

private:
    StorageReplicatedMergeTree & storage;

    /// calculate entry hash based on zookeeper path and new part name
    uint64_t getEntryHash(const ReplicatedMergeTreeLogEntryData & entry) const;

    std::atomic<time_t> execute_merges_on_single_replica_time_threshold = 0;
    std::atomic<time_t> last_refresh_time = 0;

    std::mutex mutex;
    // those 2 member accessed under the mutex, only when
    // execute_merges_on_single_replica_time_threshold enabled
    int current_replica_index = -1;
    std::vector<String> active_replicas;

};

}
