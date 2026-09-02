#pragma once
#include <config.h>

#if USE_AVRO

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>

namespace DB
{

/// RAII handle for the Paimon incremental-read processing lock.
///
/// The handle owns the Keeper session that created the lock node instead of
/// reaching for PaimonStreamState::keeper, which is a mutable member that a
/// concurrent query may replace with a freshly established session (see
/// PaimonStreamState::setKeeper).  Without this, a reader whose session died
/// mid-read would borrow the new session and successfully overwrite another
/// consumer's watermark or delete another consumer's lock.
///
/// Every mutation guarded by the lock must add addFenceOps to its transaction,
/// so a holder that lost the lock fails loudly instead of committing over
/// somebody else's progress.
class PaimonProcessingLock
{
public:
    PaimonProcessingLock(
        zkutil::ZooKeeperPtr keeper_,
        std::filesystem::path path_,
        String token_,
        Int64 session_id_,
        Int32 version_);

    ~PaimonProcessingLock();

    PaimonProcessingLock(const PaimonProcessingLock &) = delete;
    PaimonProcessingLock & operator=(const PaimonProcessingLock &) = delete;

    /// Assert that the lock node is still the one this handle created.
    /// Must be added to every transaction that mutates lock-protected state.
    void addFenceOps(Coordination::Requests & ops) const;

    /// The session that acquired the lock. Lock-protected state must be
    /// mutated through it, never through PaimonStreamState::keeper.
    zkutil::ZooKeeper & getKeeper() const { return *keeper; }

    const std::filesystem::path & getPath() const { return path; }

private:
    zkutil::ZooKeeperPtr keeper;
    std::filesystem::path path;
    String token;
    Int64 session_id;
    Int32 version;
};

using PaimonProcessingLockPtr = std::unique_ptr<PaimonProcessingLock>;

/// Manages the incremental read state for Paimon tables using ClickHouse Keeper.
/// This is similar to how Kafka2 stores offsets in Keeper, but for Paimon snapshot IDs.
///
/// Keeper path structure:
///   {keeper_path}/
///   ├── committed_snapshot      # Last consumed snapshot ID (advanced at read time)
///   ├── processing_lock         # Ephemeral lock to prevent concurrent incremental reads.
///   │                           # Its value is a fencing token identifying the owner:
///   │                           # {replica_name}/{server_uuid}/{keeper_session_id}
///   └── replicas/
///       └── {replica_name}/
///           └── is_active       # Ephemeral node indicating replica is active
///
/// The workflow is:
/// 1. Acquire processing_lock (ephemeral). If it exists, fail: another read is in progress.
/// 2. Read committed_snapshot from Keeper
/// 3. Find all snapshots > committed_snapshot
/// 4. Collect data files and advance committed_snapshot, fenced by the lock
/// 5. Release processing_lock (only if we still own it)
/// 6. Return data to the consumer for processing
///
/// Note: committed_snapshot is advanced before data consumption completes
/// (At-Most-Once). If processing fails after commit, the skipped snapshots
/// will not be re-read on retry.
///
/// Every disagreement between this state and reality fails closed: the watermark
/// only ever moves forward, only under a lock this server still owns, and only
/// through the session that took that lock. Nothing here silently repairs state.
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

    /// Get the last committed snapshot ID, returns nullopt if none
    std::optional<Int64> getCommittedSnapshotId() const;

    /// Acquire the processing lock (ephemeral). Throws on contention.
    /// The returned handle releases the lock when destroyed.
    PaimonProcessingLockPtr acquireProcessingLock();

    /// Advance the committed watermark, fenced by the processing lock.
    /// Throws INVALID_STATE if the lock was lost or the watermark would move backwards.
    void setCommittedSnapshot(const PaimonProcessingLock & lock, Int64 snapshot_id);

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
    static constexpr auto PROCESSING_LOCK_NODE = "processing_lock";
    static constexpr auto REPLICAS_NODE = "replicas";
    static constexpr auto IS_ACTIVE_NODE = "is_active";
};

using PaimonStreamStatePtr = std::shared_ptr<PaimonStreamState>;

}

#endif
