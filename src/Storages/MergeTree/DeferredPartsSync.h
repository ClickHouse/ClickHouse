#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <chrono>
#include <deque>
#include <unordered_set>


namespace DB
{

class MergeTreeData;

/// With `fsync_after_insert` and `group_fsync_before_insert_finish`, the inserted parts are written and committed
/// without fsync, and the files of all parts written by an insert sink are fsynced together when the sink
/// finishes, before the insert is acknowledged. Meanwhile the OS writes the data back in the background,
/// so the final fsync is cheap, and the insert does not wait for the disk after every part.
///
/// Holding the parts prevents their removal, even if they are merged in the meantime. To not hold them
/// for the whole duration of a long insert, the parts committed long enough ago are synced while the
/// insert is still running.
///
/// If the fsync of a part fails, the part is remembered, and every later insert that relies on it
/// (for example, a retry that is deduplicated against it) fails as well: after a failed fsync, the kernel
/// may report success for a new file descriptor, although the data was not written.
class DeferredPartsSync
{
public:
    /// Whether the fsync of the parts inserted into the table should be deferred.
    /// The files are reopened by their path on the local filesystem, so it is only possible
    /// if the table is stored on local disks. The parts of tables with `UNIQUE KEY` are always synced inline.
    static bool isEnabled(const MergeTreeData & storage, const StorageMetadataPtr & metadata_snapshot);

    /// Adding a part that is already queued does nothing.
    void add(MergeTreeDataPartPtr part);

    /// Sync the parts committed long enough ago.
    void syncOld();

    /// Sync all parts.
    void syncAll();

private:
    struct PartToSync
    {
        MergeTreeDataPartPtr part;
        std::chrono::steady_clock::time_point commit_time;
    };

    std::deque<PartToSync> parts;
    std::unordered_set<const IMergeTreeDataPart *> queued_parts;

    void sync(bool only_old);
};

}
