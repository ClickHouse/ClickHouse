#pragma once
#include "config.h"

#include <Common/CopyableAtomic.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Coordination/ACLMap.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperStorage_fwd.h>
#include <libnuraft/nuraft.hxx>
#include <IO/WriteBuffer.h>

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
};

static constexpr auto MAX_SUPPORTED_SNAPSHOT_VERSION = SnapshotVersion::V7;

/// What is stored in binary snapshot
template<typename Storage>
struct SnapshotDeserializationResult
{
    /// Storage
    std::unique_ptr<Storage> storage;
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
template<typename Storage>
struct KeeperStorageSnapshot
{
#if USE_ROCKSDB
    static constexpr bool use_rocksdb = std::is_same_v<Storage, KeeperRocksStorage>;
#else
    static constexpr bool use_rocksdb = false;
#endif

public:
    KeeperStorageSnapshot(Storage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_);

    KeeperStorageSnapshot(
        Storage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_);

    KeeperStorageSnapshot(const KeeperStorageSnapshot<Storage>&) = delete;
    KeeperStorageSnapshot(KeeperStorageSnapshot<Storage>&&) = default;

    ~KeeperStorageSnapshot();

    static void serialize(const KeeperStorageSnapshot<Storage> & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context);

    static void deserialize(SnapshotDeserializationResult<Storage> & deserialization_result, ReadBuffer & in, KeeperContextPtr keeper_context, bool load_full_storage = true);

    Storage * storage;

    SnapshotVersion version;
    /// Snapshot metadata
    SnapshotMetadataPtr snapshot_meta;
    /// Max session id
    int64_t session_id;
    /// Size of snapshot container in amount of nodes after begin iterator
    /// so we have for loop for (i = 0; i < snapshot_container_size; ++i) { doSmth(begin + i); }
    size_t snapshot_container_size;
    /// Iterator to the start of the storage
    Storage::Container::const_iterator begin;
    /// Active sessions and their timeouts
    SessionAndTimeout session_and_timeout;
    /// Sessions credentials
    Storage::SessionAndAuth session_and_auth;
    /// ACLs cache for better performance. Without we cannot deserialize storage.
    std::unordered_map<ACLId, Coordination::ACLs> acl_map;
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
};

using SnapshotFileInfoPtr = std::shared_ptr<SnapshotFileInfo>;
#if USE_ROCKSDB
using KeeperStorageSnapshotPtr = std::variant<std::shared_ptr<KeeperStorageSnapshot<KeeperMemoryStorage>>, std::shared_ptr<KeeperStorageSnapshot<KeeperRocksStorage>>>;
#else
using KeeperStorageSnapshotPtr = std::variant<std::shared_ptr<KeeperStorageSnapshot<KeeperMemoryStorage>>>;
#endif
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
template<typename Storage>
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
    SnapshotDeserializationResult<Storage> restoreFromLatestSnapshot();

    /// Compress snapshot and serialize it to buffer
    nuraft::ptr<nuraft::buffer> serializeSnapshotToBuffer(const KeeperStorageSnapshot<Storage> & snapshot) const;

    /// Serialize already compressed snapshot to disk (return path)
    SnapshotFileInfoPtr serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx);

    /// Chunked snapshot receive: open the snapshot file for writing and return a receive context.
    /// The caller appends chunks and calls finalizeSnapshotReceiveToDisk when done.
    std::unique_ptr<SnapshotReceiveCtx> beginSnapshotReceiveToDisk(uint64_t up_to_log_idx);

    /// Finalize chunked receive: sync, finalize write buffer, remove tmp marker, register snapshot.
    SnapshotFileInfoPtr finalizeSnapshotReceiveToDisk(SnapshotReceiveCtx & ctx);

    /// Serialize snapshot directly to disk
    SnapshotFileInfoPtr serializeSnapshotToDisk(const KeeperStorageSnapshot<Storage> & snapshot);

    SnapshotDeserializationResult<Storage> deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer, bool load_full_storage = true) const;

    /// Deserialize snapshot with log index up_to_log_idx from disk into compressed nuraft buffer.
    nuraft::ptr<nuraft::buffer> deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const;

    /// Deserialize latest snapshot from disk into compressed nuraft buffer.
    nuraft::ptr<nuraft::buffer> deserializeLatestSnapshotBufferFromDisk();

    /// Remove snapshot  with this log_index
    void removeSnapshot(uint64_t log_idx);

    /// Total amount of snapshots
    size_t totalSnapshots() const { return existing_snapshots.size(); }

    /// The most fresh snapshot log index we have
    size_t getLatestSnapshotIndex() const;

    SnapshotFileInfoPtr getLatestSnapshotInfo() const;

private:
    void removeOutdatedSnapshotsIfNeeded();
    void moveSnapshotsIfNeeded();

    DiskPtr getDisk() const;
    DiskPtr getLatestSnapshotDisk() const;

    /// Checks first 4 buffer bytes to became sure that snapshot compressed with
    /// ZSTD codec.
    static bool isZstdCompressed(nuraft::ptr<nuraft::buffer> buffer);

    /// How many snapshots to keep before remove
    const size_t snapshots_to_keep;
    /// All existing snapshots in our path (log_index -> path)
    std::map<uint64_t, SnapshotFileInfoPtr> existing_snapshots;
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
    KeeperStorageSnapshotPtr snapshot;
    CreateSnapshotCallback create_snapshot;
};

}
