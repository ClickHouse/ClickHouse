#pragma once
#include <Core/SettingsEnums.h>
#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/MutationCommands.h>

namespace DB
{

/// Structure representing the columns used and updated by UPDATE queries
struct UpdateAffectedColumns
{
    /// Columns update in SET clause.
    NameSet used;
    /// Columns used in any expressions.
    NameSet updated;

    /// Conflict is only when one update uses column and antoher update updates column.
    /// Updates can concurrently update the same column and can concurrently use the same column.
    bool hasConflict(const UpdateAffectedColumns & other) const;

    inline static const size_t VERSION = 0;

    String toString() const;
    void fromString(const String & str);
};

/// Structure that is used for synchronizing lightweight updates in plain MergeTree in 'auto' mode.
/// Stores counters for columns affected by currently running lightweight updates.
struct UpdateAffectedColumnsWithCounters
{
    std::unordered_map<String, UInt64> used;
    std::unordered_map<String, UInt64> updated;

    void add(const UpdateAffectedColumns & other);
    void remove(const UpdateAffectedColumns & other);
    bool hasConflict(const UpdateAffectedColumns & other) const;
};

struct PlainLightweightUpdatesSync
{
    /// Mutex for the 'sync' mode.
    std::timed_mutex sync_mutex;

    /// Mutex and structures for the 'auto' mode.
    std::mutex in_progress_mutex;
    std::condition_variable in_progress_cv;
    UpdateAffectedColumnsWithCounters in_progress_columns;

    void lockColumns(const UpdateAffectedColumns & affected_columns, size_t timeout_ms);
    void releaseColumns(const UpdateAffectedColumns & affected_columns);
};

struct PlainLightweightUpdateLock
{
    UpdateAffectedColumns affected_columns;
    std::unique_lock<std::timed_mutex> sync_lock;
    PlainLightweightUpdatesSync * lightweight_updates_sync{};

    ~PlainLightweightUpdateLock();
};

struct PlainLightweightUpdateHolder
{
    std::unique_ptr<PlainLightweightUpdateLock> update_lock;
    std::unique_ptr<PlainCommittingBlockHolder> block_holder;
};

struct LightweightUpdateHolderInKeeper
{
    void reset();

    zkutil::ZooKeeperPtr zookeeper;
    zkutil::EphemeralNodeHolderPtr lock;
    PartitionBlockNumbersHolder partition_block_numbers;
};

UpdateAffectedColumns getUpdateAffectedColumns(const MutationCommands & commands, const ContextPtr & context);

/// Get lock for lighweight mode in zookeeper.
/// Returns an ephemeral node that holds the lock in one of the three modes (see setting update_parallel_mode):
/// * async - do not synchronize updates, returns nullptr.
///
/// * sync - synchronize all updates on ephemeral node "/lightweight_updates/lock". 1 RTT in optimistic case.
///
/// * auto - synchronize only updates that have conflicts between affected columns.
/// Path "/lightweight_updates/in_progress/" contains ephemeral sequential nodes for
/// currently running lightweight updates. Each node stores affected columns by update.
///
/// The lock algorithm is the following:
///   - List "/lightweight_updates/in_progress/" and read columns affected by in-progress updates.
///   - If there is no conflict, create ephemeral sequential node "/lightweight_updates/in_progress/update-" with affected columns.
///   - If there is conflict or another lightweight update committed in meanwhile, retry.
/// 3 RTT in optimistic case.
zkutil::EphemeralNodeHolderPtr getLockForLightweightUpdateInKeeper(
    const MutationCommands & commands,
    const ContextPtr & context,
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_path);

}
