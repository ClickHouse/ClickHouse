#pragma once
#include <filesystem>
#include <system_error>
#include <Coordination/KeeperStorage.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <libnuraft/nuraft.hxx>
#include <Coordination/KeeperContext.h>

namespace DB
{

using SnapshotMetadata = nuraft::snapshot;
using SnapshotMetadataPtr = std::shared_ptr<SnapshotMetadata>;
using ClusterConfig = nuraft::cluster_config;
using ClusterConfigPtr = nuraft::ptr<ClusterConfig>;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ACL map
    V2 = 2, /// with 64 bit buffer header
    V3 = 3, /// compress snapshots with ZSTD codec
    V4 = 4, /// add Node size to snapshots
    V5 = 5, /// add ZXID and digest to snapshots
};

static constexpr auto CURRENT_SNAPSHOT_VERSION = SnapshotVersion::V5;

/// What is stored in binary shapsnot
struct SnapshotDeserializationResult
{
    /// Storage
    KeeperStoragePtr storage;
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
struct KeeperStorageSnapshot
{
public:
    KeeperStorageSnapshot(KeeperStorage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_ = nullptr);

    KeeperStorageSnapshot(
        KeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_ = nullptr);

    ~KeeperStorageSnapshot();

    static void serialize(const KeeperStorageSnapshot & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context);

    static void deserialize(SnapshotDeserializationResult & deserialization_result, ReadBuffer & in, KeeperContextPtr keeper_context);

    KeeperStorage * storage;

    SnapshotVersion version = CURRENT_SNAPSHOT_VERSION;
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
    std::unordered_map<uint64_t, Coordination::ACLs> acl_map;
    /// Cluster config from snapshot, can be empty
    ClusterConfigPtr cluster_config;
    /// Last committed ZXID
    int64_t zxid;
    /// Current digest of committed nodes
    uint64_t nodes_digest;
};

using KeeperStorageSnapshotPtr = std::shared_ptr<KeeperStorageSnapshot>;
using CreateSnapshotCallback = std::function<void(KeeperStorageSnapshotPtr &&)>;


using SnapshotMetaAndStorage = std::pair<SnapshotMetadataPtr, KeeperStoragePtr>;

/// Class responsible for snapshots serialization and deserialization. Each snapshot
/// has it's path on disk and log index.
class KeeperSnapshotManager
{
public:
    KeeperSnapshotManager(
        const std::string & snapshots_path_,
        size_t snapshots_to_keep_,
        const KeeperContextPtr & keeper_context_,
        bool compress_snapshots_zstd_ = true,
        const std::string & superdigest_ = "",
        size_t storage_tick_time_ = 500);

    /// Restore storage from latest available snapshot
    SnapshotDeserializationResult restoreFromLatestSnapshot();

    /// Compress snapshot and serialize it to buffer
    nuraft::ptr<nuraft::buffer> serializeSnapshotToBuffer(const KeeperStorageSnapshot & snapshot) const;

    /// Serialize already compressed snapshot to disk (return path)
    std::string serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx);

    /// Serialize snapshot directly to disk
    std::pair<std::string, std::error_code> serializeSnapshotToDisk(const KeeperStorageSnapshot & snapshot);

    SnapshotDeserializationResult deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const;

    /// Deserialize snapshot with log index up_to_log_idx from disk into compressed nuraft buffer.
    nuraft::ptr<nuraft::buffer> deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const;

    /// Deserialize latest snapshot from disk into compressed nuraft buffer.
    nuraft::ptr<nuraft::buffer> deserializeLatestSnapshotBufferFromDisk();

    /// Remove snapshot  with this log_index
    void removeSnapshot(uint64_t log_idx);

    /// Total amount of snapshots
    size_t totalSnapshots() const { return existing_snapshots.size(); }

    /// The most fresh snapshot log index we have
    size_t getLatestSnapshotIndex() const
    {
        if (!existing_snapshots.empty())
            return existing_snapshots.rbegin()->first;
        return 0;
    }

    std::string getLatestSnapshotPath() const
    {
        if (!existing_snapshots.empty())
        {
            const auto & path = existing_snapshots.at(getLatestSnapshotIndex());
            std::error_code ec;
            if (std::filesystem::exists(path, ec))
                return path;
        }
        return "";
    }

private:
    void removeOutdatedSnapshotsIfNeeded();

    /// Checks first 4 buffer bytes to became sure that snapshot compressed with
    /// ZSTD codec.
    static bool isZstdCompressed(nuraft::ptr<nuraft::buffer> buffer);

    const std::string snapshots_path;
    /// How many snapshots to keep before remove
    const size_t snapshots_to_keep;
    /// All existing snapshots in our path (log_index -> path)
    std::map<uint64_t, std::string> existing_snapshots;
    /// Compress snapshots in common ZSTD format instead of custom ClickHouse block LZ4 format
    const bool compress_snapshots_zstd;
    /// Superdigest for deserialization of storage
    const std::string superdigest;
    /// Storage sessions timeout check interval (also for deserializatopn)
    size_t storage_tick_time;

    KeeperContextPtr keeper_context;
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
