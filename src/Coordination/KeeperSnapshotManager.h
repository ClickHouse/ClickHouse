#pragma once
#include <Coordination/KeeperStorage.h>
#include <Common/CopyableAtomic.h>
#include <libnuraft/nuraft.hxx>

namespace DB
{

using SnapshotMetadata = nuraft::snapshot;
using SnapshotMetadataPtr = std::shared_ptr<SnapshotMetadata>;
using ClusterConfig = nuraft::cluster_config;
using ClusterConfigPtr = nuraft::ptr<ClusterConfig>;

class WriteBuffer;
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
};

static constexpr auto CURRENT_SNAPSHOT_VERSION = SnapshotVersion::V6;

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
};

/// In memory keeper snapshot. Keeper Storage based on a hash map which can be
/// turned into snapshot mode. This operation is fast and KeeperStorageSnapshot
/// class do it in constructor. It also copies iterators from storage hash table
/// up to some log index with lock. In destructor this class turn off snapshot
/// mode for KeeperStorage.
///
/// This representation of snapshot have to be serialized into NuRaft
/// buffer and send over network or saved to file.
template<typename Storage>
struct KeeperStorageSnapshot
{
#if USE_ROCKSDB
    static constexpr bool use_rocksdb = std::is_same_v<Storage, KeeperRocksStorage>;
#else
    static constexpr bool use_rocksdb = false;
#endif

public:
    KeeperStorageSnapshot(Storage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_ = nullptr);

    KeeperStorageSnapshot(
        Storage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_ = nullptr);

    KeeperStorageSnapshot(const KeeperStorageSnapshot<Storage>&) = delete;
    KeeperStorageSnapshot(KeeperStorageSnapshot<Storage>&&) = default;

    ~KeeperStorageSnapshot();

    static void serialize(const KeeperStorageSnapshot<Storage> & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context);

    static void deserialize(SnapshotDeserializationResult<Storage> & deserialization_result, ReadBuffer & in, KeeperContextPtr keeper_context);

    Storage * storage;

    SnapshotVersion version = CURRENT_SNAPSHOT_VERSION;
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
    std::unordered_map<uint64_t, Coordination::ACLs> acl_map;
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
    mutable std::atomic<size_t> size{0};
};

using SnapshotFileInfoPtr = std::shared_ptr<SnapshotFileInfo>;
#if USE_ROCKSDB
using KeeperStorageSnapshotPtr = std::variant<std::shared_ptr<KeeperStorageSnapshot<KeeperMemoryStorage>>, std::shared_ptr<KeeperStorageSnapshot<KeeperRocksStorage>>>;
#else
using KeeperStorageSnapshotPtr = std::variant<std::shared_ptr<KeeperStorageSnapshot<KeeperMemoryStorage>>>;
#endif
using CreateSnapshotCallback = std::function<SnapshotFileInfoPtr(KeeperStorageSnapshotPtr &&, bool)>;

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

    /// Serialize snapshot directly to disk
    SnapshotFileInfoPtr serializeSnapshotToDisk(const KeeperStorageSnapshot<Storage> & snapshot);

    SnapshotDeserializationResult<Storage> deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const;

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

/// Keeper create snapshots in background thread. KeeperStateMachine just create
/// in-memory snapshot from storage and push task for it serialization into
/// special tasks queue. Background thread check this queue and after snapshot
/// successfully serialized notify state machine.
struct CreateSnapshotTask
{
    KeeperStorageSnapshotPtr snapshot;
    CreateSnapshotCallback create_snapshot;
};

}
