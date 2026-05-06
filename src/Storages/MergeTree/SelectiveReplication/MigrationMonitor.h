#pragma once

#include <base/types.h>
#include <Common/Logger.h>

#include <memory>


namespace DB
{

class StorageReplicatedMergeTree;

namespace SelectiveReplication
{

/// Monitors partition migrations and drives the migration state machine
/// for selective replication. Extracted from
/// `StorageReplicatedMergeTree` to keep migration logic focused,
/// following the `MergeTreeDataSelectExecutor` pattern: holds a storage
/// reference and is declared as `friend class` in
/// `StorageReplicatedMergeTree`.
class MigrationMonitor
{
public:
    explicit MigrationMonitor(StorageReplicatedMergeTree & storage_);

    /// Background task entry point. Cleans up unassigned parts, runs
    /// one migration cycle, then re-schedules itself.
    void run();

    /// Drive one cycle of the migration coordinator: own migrations,
    /// clone completion, orphan recovery, and optionally rebalance.
    void runMigrationCycle(bool force_rebalance);

    /// SYSTEM START SELECTIVE REBALANCE entry point.
    void triggerRebalance();

    /// SYSTEM SYNC SELECTIVE MIGRATIONS entry point. Blocks until all
    /// active migrations complete or `timeout_ms` elapses. Returns true
    /// if all finished, false on timeout.
    bool waitForMigrationsComplete(UInt64 timeout_ms);

private:
    StorageReplicatedMergeTree & storage;
    LoggerPtr log;
};

}
}
