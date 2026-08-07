#pragma once
#include <config.h>

#if USE_AVRO

#include <filesystem>
#include <mutex>
#include <optional>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Manages the incremental read state for Paimon tables using ClickHouse Keeper.
/// This is similar to how Kafka2 stores offsets in Keeper, but for Paimon snapshot IDs.
///
/// Keeper path structure:
///   {keeper_path}/
///   ├── committed_snapshot      # Last consumed snapshot ID (advanced at read time)
///   ├── committed_table_identity # `schema-0` `timeMillis` of the table generation the
///   │                            # watermark above belongs to (written in the same transaction)
///   ├── processing_lock         # Ephemeral lock to prevent concurrent incremental reads
///   └── replicas/
///       └── {replica_name}/
///           └── is_active       # Ephemeral node indicating replica is active
///
/// The workflow is:
/// 1. Read committed_snapshot from Keeper
/// 2. Find all snapshots > committed_snapshot
/// 3. Acquire processing_lock (ephemeral). If exists, wait/fail/cleanup stale.
/// 4. Collect data files and advance committed_snapshot
/// 5. Release processing_lock (delete or session-expire)
/// 6. Return data to the consumer for processing
///
/// Note: committed_snapshot is advanced before data consumption completes
/// (At-Most-Once). If processing fails after commit, the skipped snapshots
/// will not be re-read on retry.
///
/// The watermark is committed together with a marker of the table generation
/// it belongs to (the schema-0 `timeMillis`). A watermark carrying no marker
/// or a foreign generation is discarded by PaimonMetadata::getCommittedSnapshotId
/// (see PaimonMetadata::isCommittedWatermarkFromSameTable), so the snapshot
/// current at that moment is re-delivered in full once — a bounded, one-time
/// exception to the no-re-read rule above, preferred over silently skipping
/// data that may belong to a different table.
class PaimonStreamState
{
public:
    PaimonStreamState(
        zkutil::ZooKeeperPtr keeper_,
        const String & keeper_path_,
        const String & replica_name_,
        LoggerPtr log_);

    ~PaimonStreamState();

    /// Check if Keeper session needs to be refreshed
    bool needsNewKeeper() const;

    /// Set new Keeper session
    void setKeeper(zkutil::ZooKeeperPtr keeper_);

    /// Get the last committed snapshot ID, returns -1 if none
    std::optional<Int64> getCommittedSnapshotId() const;

    /// Get the table-generation marker (`schema-0` `timeMillis`) the committed watermark was
    /// persisted for, if it was recorded.  Absent for a watermark written before identity
    /// tracking existed, or when the identity was not latched at create time.
    std::optional<Int64> getCommittedTableIdentity() const;

    /// Acquire processing lock (ephemeral). Throws on contention.
    void acquireProcessingLock();

    /// Release processing lock (best-effort).
    void releaseProcessingLock();

    /// Commit snapshot as successfully processed, together with the table generation
    /// (`schema-0` `timeMillis`) it belongs to.  Pass 0 when the identity is not latched.
    void setCommittedSnapshot(Int64 snapshot_id, Int64 table_identity);

    /// Initialize Keeper nodes if they don't exist
    void initializeKeeperNodes();

    /// Activate this replica (create ephemeral is_active node)
    bool activate();

    /// Deactivate this replica
    void deactivate();

    /// Check if this replica is active
    bool isActive() const { return is_active; }

    /// Get keeper path
    const String & getKeeperPath() const { return keeper_path; }

private:
    /// Remove processing lock if exists (used for cleanup)
    void removeProcessingLock();

    /// Write a value to Keeper node (create or update)
    void writeToKeeper(const std::filesystem::path & path, const String & value);

    /// Read a value from Keeper node
    std::optional<String> readFromKeeper(const std::filesystem::path & path) const;

    mutable std::mutex mutex;
    zkutil::ZooKeeperPtr keeper;
    const String keeper_path;
    const String replica_name;
    const std::filesystem::path fs_keeper_path;
    LoggerPtr log;
    /// Unique identifier for this server instance, used for ownership
    /// checks when reclaiming stale is_active ephemeral nodes.
    /// Generated from ServerUUID so it survives server restarts
    /// but differs across distinct servers.
    const String active_node_identifier;

    std::atomic<bool> is_active{false};
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;

    // Keeper node names
    static constexpr auto COMMITTED_SNAPSHOT_NODE = "committed_snapshot";
    static constexpr auto COMMITTED_TABLE_IDENTITY_NODE = "committed_table_identity";
    static constexpr auto PROCESSING_LOCK_NODE = "processing_lock";
    static constexpr auto REPLICAS_NODE = "replicas";
    static constexpr auto IS_ACTIVE_NODE = "is_active";
};

using PaimonStreamStatePtr = std::shared_ptr<PaimonStreamState>;

}

#endif

