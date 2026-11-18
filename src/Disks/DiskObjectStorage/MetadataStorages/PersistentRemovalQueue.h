#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IRemovalQueue.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>
#include <Disks/IDisk.h>

#include <Common/Logger.h>

namespace DB
{

class PersistentRemovalQueue final : public IRemovalQueue
{
public:
    PersistentRemovalQueue(DiskPtr disk_, const std::string & directory, size_t compaction_threshold_);

    void load() override;

    void submitForRemoval(const StoredObjects & blobs) override;

    StoredObjects takeFirst(int64_t max_count) const override { return queue.takeFirst(max_count); }

    int64_t markAsRemoved(const StoredObjects & blobs) override;

    size_t size() const override { return queue.size(); }

private:
    enum Version : UInt32
    {
        V0 = 0, /// initial binary format
    };

    static constexpr Version CURRENT_VERSION = Version::V0;

    enum EntryType : UInt8
    {
        ADD = 0,
        REMOVED = 1,
    };

    /// Returns true if compaction is needed (version mismatch or truncated entries).
    bool readLog();
    void appendToLog(EntryType entry_type, const StoredObjects & blobs);
    void compactLog();

    const DiskPtr disk;
    const std::string directory;
    const std::string log_path;
    const size_t compaction_threshold;
    const LoggerPtr log;

    InMemoryRemovalQueue queue;

    /// Number of REMOVED entries in the log that haven't been compacted yet.
    size_t stale_entries = 0;
};

}
