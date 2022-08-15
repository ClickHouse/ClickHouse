#pragma once
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Coordination/KeeperStorage.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

using SnapshotMetadata = nuraft::snapshot;
using SnapshotMetadataPtr = std::shared_ptr<SnapshotMetadata>;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ACL map
    V2 = 2, /// with 64 bit buffer header
};

static constexpr auto CURRENT_SNAPSHOT_VERSION = SnapshotVersion::V2;

struct KeeperStorageSnapshot
{
public:
    KeeperStorageSnapshot(KeeperStorage * storage_, uint64_t up_to_log_idx_);

    KeeperStorageSnapshot(KeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_);
    ~KeeperStorageSnapshot();

    static void serialize(const KeeperStorageSnapshot & snapshot, WriteBuffer & out);

    static SnapshotMetadataPtr deserialize(KeeperStorage & storage, ReadBuffer & in);

    KeeperStorage * storage;

    SnapshotVersion version = CURRENT_SNAPSHOT_VERSION;
    SnapshotMetadataPtr snapshot_meta;
    int64_t session_id;
    size_t snapshot_container_size;
    KeeperStorage::Container::const_iterator begin;
    SessionAndTimeout session_and_timeout;
    KeeperStorage::SessionAndAuth session_and_auth;
    std::unordered_map<uint64_t, Coordination::ACLs> acl_map;
};

using KeeperStorageSnapshotPtr = std::shared_ptr<KeeperStorageSnapshot>;
using CreateSnapshotCallback = std::function<void(KeeperStorageSnapshotPtr &&)>;


using SnapshotMetaAndStorage = std::pair<SnapshotMetadataPtr, KeeperStoragePtr>;

class KeeperSnapshotManager
{
public:
    KeeperSnapshotManager(const std::string & snapshots_path_, size_t snapshots_to_keep_, const std::string & superdigest_ = "", size_t storage_tick_time_ = 500);

    SnapshotMetaAndStorage restoreFromLatestSnapshot();

    static nuraft::ptr<nuraft::buffer> serializeSnapshotToBuffer(const KeeperStorageSnapshot & snapshot);
    std::string serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx);

    SnapshotMetaAndStorage deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const;

    nuraft::ptr<nuraft::buffer> deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const;
    nuraft::ptr<nuraft::buffer> deserializeLatestSnapshotBufferFromDisk();

    void removeSnapshot(uint64_t log_idx);

    size_t totalSnapshots() const
    {
        return existing_snapshots.size();
    }

    size_t getLatestSnapshotIndex() const
    {
        if (!existing_snapshots.empty())
            return existing_snapshots.rbegin()->first;
        return 0;
    }

private:
    void removeOutdatedSnapshotsIfNeeded();
    const std::string snapshots_path;
    const size_t snapshots_to_keep;
    std::map<uint64_t, std::string> existing_snapshots;
    const std::string superdigest;
    size_t storage_tick_time;
};

struct CreateSnapshotTask
{
    KeeperStorageSnapshotPtr snapshot;
    CreateSnapshotCallback create_snapshot;
};

}
