#pragma once
#include "config.h"

#include <atomic>
#include <Common/CopyableAtomic.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Coordination/ACLMap.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperStorage.h>
#include <functional>
#include <libnuraft/nuraft.hxx>
#include <IO/WriteBuffer.h>
#include <vector>

#include <mutex>

namespace DB
{

using SnapshotMetadata = nuraft::snapshot;
using SnapshotMetadataPtr = std::shared_ptr<SnapshotMetadata>;
using ClusterConfig = nuraft::cluster_config;
using ClusterConfigPtr = nuraft::ptr<ClusterConfig>;

class ReadBuffer;

class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ACL map
    V2 = 2, /// with 64 bit buffer header
    V3 = 3, /// compress snapshots with ZSTD codec
    V4 = 4, /// add Node size to snapshots
    V5 = 5, /// add ZXID and digest to snapshots
    V6 = 6, /// remove is_sequential, per node size, data length
    V7 = 7, /// acl_id narrowed from uint64_t to uint32_t, seq_num widened from int32_t to int64_t
    V8 = 8, /// add destroy_time and ttl for TTL nodes
};

static constexpr auto MAX_SUPPORTED_SNAPSHOT_VERSION = SnapshotVersion::V8;

/// What is stored in binary snapshot
struct SnapshotDeserializationResult
{
    /// Storage
    std::unique_ptr<KeeperStorage> storage;
    /// Snapshot metadata (up_to_log_idx and so on)
    SnapshotMetadataPtr snapshot_meta;
    /// Cluster config
    ClusterConfigPtr cluster_config;
    /// Container with all the paths stored in snapshot
    /// used if we don't want to load entire storage from snapshot
    /// which can be useful for analyzing snapshot files
    std::vector<std::string> paths;
};

/// In memory keeper snapshot. Keeper Storage based on a hash map which can be
/// turned into snapshot mode. This operation is fast and KeeperStorageSnapshot
/// class does it in constructor. It also copies iterators from storage hash table
/// up to some log index with lock. In destructor this class turns off snapshot
/// mode for KeeperStorage.
///
/// This representation of snapshot has to be serialized into NuRaft
/// buffer and sent over network or saved to file.
///
/// Tricky to use correctly:
///  * During the constructor call, storage contents must not change, and up_to_log_idx_ must match
///    the storage's commit idx. In keeper server, this means that nuraft's commit_lock_ must be held.
///  * At most one instance of KeeperStorageSnapshot can exist at a time, for a given KeeperStorage.
///    NuRaft guarantees that at most one snapshotting operation can be in progress (create_snapshot
///    is not called again until when_done callback is called).
///  * Destructor must be called with storage mutex held (for the disableSnapshotMode() call).
struct KeeperStorageSnapshot
{
public:
    KeeperStorageSnapshot(KeeperStorage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_);

    KeeperStorageSnapshot(
        KeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_);

    KeeperStorageSnapshot(const KeeperStorageSnapshot &) = delete;
    KeeperStorageSnapshot(KeeperStorageSnapshot &&) = default;

    ~KeeperStorageSnapshot();

    static void serialize(const KeeperStorageSnapshot & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context);

    static void deserialize(SnapshotDeserializationResult & deserialization_result, ReadBuffer & in, KeeperContextPtr keeper_context, bool load_full_storage = true);

    KeeperStorage * storage;

    SnapshotVersion version;
    /// Snapshot metadata
    SnapshotMetadataPtr snapshot_meta;
    /// Max session id
    int64_t session_id;
    /// Size of snapshot container in amount of nodes after begin iterator
    /// so we have for loop for (i = 0; i < snapshot_container_size; ++i) { doSmth(begin + i); }
    size_t snapshot_container_size;
    /// Iterator to the start of the storage
    KeeperStorage::Container::const_iterator begin;
    /// Active sessions and their timeouts
    SessionAndTimeout session_and_timeout;
    /// Sessions credentials
    KeeperStorage::SessionAndAuth session_and_auth;
    /// ACLs cache for better performance. Without we cannot deserialize storage.
    std::vector<std::pair<ACLId, Coordination::ACLs>> acl_map;
    /// Cluster config from snapshot, can be empty
    ClusterConfigPtr cluster_config;
    /// Last committed ZXID
    int64_t zxid;
    /// Current digest of committed nodes
    uint64_t nodes_digest;
};

struct SnapshotFileInfo
{
    SnapshotFileInfo(std::string path_, DiskPtr disk_)
        : path(std::move(path_))
        , disk(std::move(disk_))
    {}

    std::string path;
    DiskPtr disk;

    /// Set when the file should be unlinked after the last `shared_ptr` drops.
    /// A false value keeps the file across manager destruction.
    std::atomic<bool> retired_for_removal{false};
};

using SnapshotFileInfoPtr = std::shared_ptr<SnapshotFileInfo>;

struct SnapshotMoveCandidate
{
    uint64_t log_idx = 0;
    SnapshotFileInfoPtr file_info;
    DiskPtr source_disk;
    std::string source_path;
    DiskPtr target_disk;
    std::string target_path;
};

struct SnapshotMaintenanceTasks
{
    std::vector<SnapshotFileInfoPtr> retired_snapshots;
    std::vector<SnapshotMoveCandidate> move_candidates;
};

using KeeperStorageSnapshotPtr = std::shared_ptr<KeeperStorageSnapshot>;
using CreateSnapshotCallback = std::function<SnapshotFileInfoPtr(KeeperStorageSnapshotPtr &&, bool)>;

/// In-progress chunked snapshot receive state on the follower side.
/// Holds the write buffer for writing chunks directly to disk and the tmp_
/// marker path used to detect incomplete writes on restart.
struct SnapshotReceiveCtx
{
    const uint64_t log_idx = 0;
    /// The obj_id of the next chunk we expect to receive. Starts at 0, incremented after
    /// each chunk is written. Used to detect duplicate or out-of-order chunks.
    uint64_t expected_obj_id = 0;
    const std::string snapshot_file_name;
    const DiskPtr disk;
    std::unique_ptr<WriteBuffer> write_buf;

    SnapshotReceiveCtx(
        std::unique_ptr<WriteBuffer> write_buf_,
        DiskPtr disk_,
        std::string snapshot_file_name_,
        uint64_t log_idx_)
        : log_idx(log_idx_)
        , expected_obj_id(0)
        , snapshot_file_name(std::move(snapshot_file_name_))
        , disk(std::move(disk_))
        , write_buf(std::move(write_buf_))
    {
    }

    /// Cancel write buffer if not finalized.
    ~SnapshotReceiveCtx()
    {
        if (write_buf && !write_buf->isFinalized())
            write_buf->cancel();
    }

};

/// Class responsible for snapshots serialization and deserialization. Each snapshot
/// has it's path on disk and log index.
class KeeperSnapshotManager
{
public:
    KeeperSnapshotManager(
        size_t snapshots_to_keep_,
        const KeeperContextPtr & keeper_context_,
        bool compress_snapshots_zstd_ = true,
        const std::string & superdigest_ = "",
        size_t storage_tick_time_ = 500);

    /// Restore storage from latest available snapshot
    SnapshotDeserializationResult restoreFromLatestSnapshot();

    /// Compress snapshot and serialize it to buffer
    nuraft::ptr<nuraft::buffer> serializeSnapshotToBuffer(const KeeperStorageSnapshot & snapshot) const;

    /// Write helpers do disk I/O under a fresh unique name and do not publish metadata.
    SnapshotFileInfoPtr writeSnapshotFile(const KeeperStorageSnapshot & snapshot);
    SnapshotFileInfoPtr writeSnapshotBufferToFile(nuraft::buffer & buffer, uint64_t up_to_log_idx);

    /// Returns the already-registered entry for `up_to_log_idx` so the caller can skip rewriting.
    /// Returns nullptr if nothing is registered.
    SnapshotFileInfoPtr tryReuseRegisteredSnapshot(uint64_t up_to_log_idx) const;

    /// Publish `written`, retire it if a race was lost, run inline maintenance, and return the
    /// published metadata. Used by write-from-scratch and receive paths (not by
    /// `KeeperStateMachine::publishWrittenSnapshot`, which calls `publishSnapshotFile` directly).
    SnapshotFileInfoPtr publishAndRunMaintenance(uint64_t up_to_log_idx, SnapshotFileInfoPtr written);

    /// Metadata-only; call under `IKeeperStateMachine::snapshots_lock`.
    /// Returns the already-registered entry for the same log index if any; caller retires its file on loss.
    SnapshotFileInfoPtr publishSnapshotFile(uint64_t up_to_log_idx, SnapshotFileInfoPtr file_info);
    /// `just_written_log_idx` (0 = none) pins the caller's own entry through this pass.
    SnapshotMaintenanceTasks prepareSnapshotMaintenanceTasks(uint64_t just_written_log_idx);
    bool publishMovedSnapshotIfValid(const SnapshotMoveCandidate & candidate);
    void retireUnpublishedSnapshotFile(const SnapshotFileInfoPtr & file_info) const;

    /// Cross-disk copy/removal; must run outside `snapshots_lock`.
    bool moveSnapshotCandidate(
        const SnapshotMoveCandidate & candidate,
        const std::function<bool(const SnapshotMoveCandidate &)> & publish_moved_snapshot);

    /// Best-effort maintenance (swallows exceptions); `just_written_log_idx` (0 = none) pins the
    /// caller's own entry through this pass.
    void runMaintenanceInline(uint64_t just_written_log_idx);

    /// Serialize already compressed snapshot to disk (return path)
    SnapshotFileInfoPtr serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx);

    /// Chunked receive: open a snapshot file under a fresh unique name and return a context.
    std::unique_ptr<SnapshotReceiveCtx> beginSnapshotReceiveToDisk(uint64_t up_to_log_idx);

    /// Finalize chunked receive: sync, finalize write buffer, remove tmp marker, register snapshot.
    SnapshotFileInfoPtr finalizeSnapshotReceiveToDisk(SnapshotReceiveCtx & ctx);

    /// Serialize snapshot directly to disk
    SnapshotFileInfoPtr serializeSnapshotToDisk(const KeeperStorageSnapshot & snapshot);

    SnapshotDeserializationResult deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer, bool load_full_storage = true) const;

    SnapshotMetadataPtr deserializeSnapshotMetadataFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const;

    /// Deserialize pinned snapshot from disk into compressed nuraft buffer.
    nuraft::ptr<nuraft::buffer> deserializeSnapshotBufferFromDisk(const SnapshotFileInfo & snapshot_info) const;

    /// Deserialize snapshot with log index up_to_log_idx from disk into compressed nuraft buffer.
    nuraft::ptr<nuraft::buffer> deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const;

    /// Deserialize latest snapshot from disk into compressed nuraft buffer.
    nuraft::ptr<nuraft::buffer> deserializeLatestSnapshotBufferFromDisk();

    /// Remove snapshot with this log_index. Used by tests and tools only.
    void removeSnapshot(uint64_t log_idx);

    /// Total amount of snapshots
    size_t totalSnapshots() const { return existing_snapshots.size(); }

    /// The most fresh snapshot log index we have
    size_t getLatestSnapshotIndex() const;

    SnapshotFileInfoPtr getLatestSnapshotInfo() const;

    std::map<uint64_t, SnapshotFileInfoPtr> getExistingSnapshots(const std::lock_guard<std::mutex> & /*snapshots_lock*/) const;

    /// Return the map entry for `log_idx`, or `nullptr` if absent. Holding the
    /// result pins the file against unlink and cross-disk moves.
    /// Caller must hold `KeeperStateMachine::snapshots_lock`.
    SnapshotFileInfoPtr getSnapshotPin(uint64_t log_idx) const;

    /// Protect this log index from retention — the file backing `latest_snapshot_meta` must
    /// stay servable to NuRaft. 0 = none. Caller must hold `KeeperStateMachine::snapshots_lock`.
    void setProtectedSnapshotIndex(uint64_t log_idx);

    /// Protect a pending install from retention until `apply_snapshot` consumes it. 0 = none.
    /// Caller must hold `KeeperStateMachine::snapshots_lock`.
    void setProtectedPendingSnapshotIndex(uint64_t log_idx);

private:
    /// Detach the entry at `it` and same-index recovery copies, marking retired; caller's drop unlinks them.
    std::vector<SnapshotFileInfoPtr> detachSnapshotForRemoval(std::map<uint64_t, SnapshotFileInfoPtr>::iterator it);
    /// `just_written_log_idx` (0 = none) pins the calling writer's own entry through this pass.
    std::vector<SnapshotFileInfoPtr> detachOutdatedSnapshotsIfNeeded(uint64_t just_written_log_idx);
    std::vector<SnapshotMoveCandidate> selectSnapshotsToMove();
    void cleanupCopiedMoveTarget(const SnapshotMoveCandidate & candidate) const;

    /// Build a `shared_ptr<SnapshotFileInfo>` whose deleter unlinks only when
    /// `retired_for_removal` is set.
    SnapshotFileInfoPtr makeManagedSnapshotFileInfo(std::string path, DiskPtr disk, uint64_t log_idx) const;

    DiskPtr getDisk() const;
    DiskPtr getLatestSnapshotDisk() const;

    /// Checks first 4 buffer bytes to became sure that snapshot compressed with
    /// ZSTD codec.
    static bool isZstdCompressed(nuraft::ptr<nuraft::buffer> buffer);

    /// How many snapshots to keep before remove
    const size_t snapshots_to_keep;
    /// All existing snapshots in our path (log_index -> path)
    std::map<uint64_t, SnapshotFileInfoPtr> existing_snapshots;
    /// Same-index recovery copies kept on disk at startup but NOT registered in
    /// `existing_snapshots` (retained-window duplicates + re-point orphans), keyed by log index.
    /// `detachSnapshotForRemoval` retires them with their index so they age out without a restart.
    std::unordered_map<uint64_t, std::vector<SnapshotFileInfoPtr>> retained_duplicate_snapshots;
    /// See `setProtectedSnapshotIndex` / `setProtectedPendingSnapshotIndex`. Both checked by
    /// `detachOutdatedSnapshotsIfNeeded`.
    uint64_t protected_snapshot_log_idx = 0;
    uint64_t protected_pending_snapshot_log_idx = 0;
    /// Compress snapshots in common ZSTD format instead of custom ClickHouse block LZ4 format
    const bool compress_snapshots_zstd;
    /// Superdigest for deserialization of storage
    const std::string superdigest;
    /// Storage sessions timeout check interval (also for deserializatopn)
    size_t storage_tick_time;

    KeeperContextPtr keeper_context;

    LoggerPtr log = getLogger("KeeperSnapshotManager");
};

/// Keeper creates snapshots in background thread. KeeperStateMachine just creates
/// in-memory snapshot from storage and pushes task for its serialization into
/// special tasks queue. Background thread checks this queue and after snapshot
/// successfully serialized notifies state machine.
struct CreateSnapshotTask
{
    /// Declared before `snapshot`: the closure keeps the captured storage alive while the
    /// snapshot holds a raw pointer into it; reverse member destruction protects a task
    /// that is destroyed without being executed.
    CreateSnapshotCallback create_snapshot;
    KeeperStorageSnapshotPtr snapshot;
};

}
