#pragma once

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>

#include <vector>
#include <unordered_set>

namespace DB
{

class StorageReplicatedMergeTree;

/// Metadata of a single migration stored in ZooKeeper as a single JSON node.
struct MigrationMetadata
{
    String state;
    String partition_id;
    String source_replica;
    String target_replica;
    String coordinator;
    String created_at;
    Strings source_parts_snapshot;

    /// The shared log entry number just before GET_PART entries were written.
    /// Used to detect partial initialization (crash between migration node creation
    /// and GET_PART log writes). Empty string means pre-upgrade migration (skip check).
    String log_pointer_at_start;

    /// Serialization format version for forward compatibility.
    static constexpr int FORMAT_VERSION = 1;

    /// Serialize to JSON string.
    String serialize() const;

    /// Deserialize from JSON string. Returns false if data is empty or unparsable.
    static bool deserialize(const String & data, MigrationMetadata & out);

    /// Read from ZK (single node at migration_path). Returns false if node doesn't exist.
    /// If stat_out is provided, the ZK node stat is written to it.
    static bool read(const zkutil::ZooKeeperPtr & zk, const String & migration_path,
                     MigrationMetadata & out, Coordination::Stat * stat_out = nullptr);

    /// CAS-update state field in ZK. Returns true on success.
    static bool updateState(
        const zkutil::ZooKeeperPtr & zk,
        const String & migration_path,
        const String & new_state);
};

/// Coordinates partition migration for selective replication.
/// Drives the CLONE -> SWITCH -> DONE state machine.
class PartitionMigrationCoordinator
{
public:
    explicit PartitionMigrationCoordinator(StorageReplicatedMergeTree & storage_);

    /// Check partition balance and start migrations if needed.
    /// Called periodically by migrationMonitorTask. Silently returns if lock is held by another replica.
    void checkAndStartAutoRebalance();

    /// Recover orphaned migrations where coordinator went offline.
    void recoverOrphanedMigrations();

    /// Drive state machine for active migrations.
    /// If own_only is true, only drives migrations where this replica is the coordinator.
    /// If own_only is false, drives ALL active migrations regardless of coordinator.
    /// Safe to call from multiple nodes because driveStateMachine uses CAS.
    void driveMigrations(bool own_only);

    /// Check if this replica (as target) has completed cloning for any active migrations
    /// and signal completion by creating the clone_complete node.
    void checkAndSignalCloneComplete();

    /// Returns partition IDs that have active migrations (CLONE or SWITCH state).
    static std::unordered_set<String> getActiveMigrationPartitions(
        const zkutil::ZooKeeperPtr & zk, const String & migrations_path);

    /// Start a CLONE phase migration for the given partition.
    /// Used by SYSTEM MIGRATE PARTITION command.
    String startClone(
        const zkutil::ZooKeeperPtr & zk,
        const String & partition_id,
        const String & source_replica,
        const String & target_replica);

private:
    struct MigrationPlan
    {
        String partition_id;
        String source_replica;
        String target_replica;
    };

    /// ---- Path helpers ----

    String migrationsPath() const;
    String migrationPath(const String & migration_id) const;
    String assignmentPath(const String & partition_id) const;
    String countsPath() const;

    /// ---- CAS helpers ----

    /// CAS-update the assignment for a partition. The `modifier` receives the current
    /// replica list and must return the new replica list (or std::nullopt to abort).
    /// Returns true on success, false if modifier returned nullopt.
    /// Throws on unexpected ZK errors or exhausted retries.
    bool casUpdateAssignment(
        const zkutil::ZooKeeperPtr & zk,
        const String & partition_id,
        std::function<std::optional<Strings>(Strings)> modifier);

    /// ---- Core state machine ----

    void driveStateMachine(const zkutil::ZooKeeperPtr & zk, const String & migration_id);
    void rollback(const zkutil::ZooKeeperPtr & zk, const String & migration_id);
    std::vector<MigrationPlan> computeRebalancePlan(const zkutil::ZooKeeperPtr & zk);

    bool isCloneComplete(const zkutil::ZooKeeperPtr & zk, const String & migration_id);
    void checkAndSignalCloneCompleteInternal(const zkutil::ZooKeeperPtr & zk);

    /// Remove migration ZK subtree on rollback or partial-init cleanup.
    void cleanupMigrationSubtree(const zkutil::ZooKeeperPtr & zk, const String & migration_path);

    /// Clean up terminated migrations (FAILED: remove :cloning from assignment + delete node;
    /// DONE: delete node). Called at the start of recoverOrphanedMigrations.
    void cleanupTerminatedMigrations(const zkutil::ZooKeeperPtr & zk);

    /// Check whether a migration in CLONE state was fully initialized
    /// (assignment updated with :cloning and GET_PART entries written).
    bool isMigrationFullyInitialized(
        const zkutil::ZooKeeperPtr & zk,
        const MigrationMetadata & meta);

    /// Take over coordination of an orphaned migration.
    /// Returns true if takeover succeeded and the state machine should be driven.
    bool tryTakeoverMigration(
        const zkutil::ZooKeeperPtr & zk,
        const String & migration_id,
        const MigrationMetadata & meta);

    /// ---- Part helpers ----

    /// Read active parts for a partition from a replica's ZK parts list.
    Strings readActivePartsForPartition(
        const zkutil::ZooKeeperPtr & zk,
        const String & replica_name,
        const String & partition_id);

    StorageReplicatedMergeTree & storage;
    LoggerPtr log;
};

}
