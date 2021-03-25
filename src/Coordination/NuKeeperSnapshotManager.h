#pragma once
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Coordination/NuKeeperStorage.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

using SnapshotMetadata = nuraft::snapshot;
using SnapshotMetadataPtr = std::shared_ptr<SnapshotMetadata>;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
};

struct NuKeeperStorageSnapshot
{
public:
    NuKeeperStorageSnapshot(NuKeeperStorage * storage_, size_t up_to_log_idx_);

    NuKeeperStorageSnapshot(NuKeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_);
    ~NuKeeperStorageSnapshot();

    static void serialize(const NuKeeperStorageSnapshot & snapshot, WriteBuffer & out);

    static SnapshotMetadataPtr deserialize(NuKeeperStorage & storage, ReadBuffer & in);

    NuKeeperStorage * storage;

    SnapshotVersion version = SnapshotVersion::V0;
    SnapshotMetadataPtr snapshot_meta;
    int64_t session_id;
    size_t snapshot_container_size;
    NuKeeperStorage::Container::const_iterator begin;
    SessionAndTimeout session_and_timeout;
};

using NuKeeperStorageSnapshotPtr = std::shared_ptr<NuKeeperStorageSnapshot>;
using CreateSnapshotCallback = std::function<void(NuKeeperStorageSnapshotPtr &&)>;

class NuKeeperSnapshotManager
{
public:
    NuKeeperSnapshotManager(const std::string & snapshots_path_, size_t snapshots_to_keep_);

    SnapshotMetadataPtr restoreFromLatestSnapshot(NuKeeperStorage * storage);

    static nuraft::ptr<nuraft::buffer> serializeSnapshotToBuffer(const NuKeeperStorageSnapshot & snapshot);
    std::string serializeSnapshotBufferToDisk(nuraft::buffer & buffer, size_t up_to_log_idx);

    static SnapshotMetadataPtr deserializeSnapshotFromBuffer(NuKeeperStorage * storage, nuraft::ptr<nuraft::buffer> buffer);

    nuraft::ptr<nuraft::buffer> deserializeSnapshotBufferFromDisk(size_t up_to_log_idx) const;
    nuraft::ptr<nuraft::buffer> deserializeLatestSnapshotBufferFromDisk();

    void removeSnapshot(size_t log_idx);

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
    std::map<size_t, std::string> existing_snapshots;
};

struct CreateSnapshotTask
{
    NuKeeperStorageSnapshotPtr snapshot;
    CreateSnapshotCallback create_snapshot;
};

}
