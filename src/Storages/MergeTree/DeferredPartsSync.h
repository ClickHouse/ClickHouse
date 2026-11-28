#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <chrono>
#include <deque>
#include <functional>
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
/// If the fsync of a file fails, the file is remembered, and every later insert that relies on it
/// (for example, a retry that is deduplicated against its part) fails as well, see `parallelSyncPaths`.
/// This is remembered only until the server restarts, so the parts with the files that failed to sync
/// are also passed to `on_failed_sync`, which can remove them from the table persistently.
class DeferredPartsSync
{
public:
    /// Whether the fsync of the parts inserted into the table should be deferred.
    /// The files are reopened by their path on the local filesystem, so it is only possible
    /// if the table is stored on local disks. The parts of tables with `UNIQUE KEY` are always synced inline.
    /// With `group_fsync_before_insert_finish = 'auto'`, it is enabled only for non-replicated tables.
    static bool isEnabled(const MergeTreeData & storage, const StorageMetadataPtr & metadata_snapshot);

    /// Whether an insert into the table must sync the existing parts that it relies on, for example, a part
    /// that its block is deduplicated against. It does not depend on `group_fsync_before_insert_finish`,
    /// because such a part may be written by another insert with a different value of this setting.
    static bool shouldSyncExistingParts(const MergeTreeData & storage);

    using OnFailedSync = std::function<void(const MergeTreeDataPartPtr &)>;

    explicit DeferredPartsSync(OnFailedSync on_failed_sync_ = {}) : on_failed_sync(std::move(on_failed_sync_)) {}
    DeferredPartsSync(const DeferredPartsSync &) = delete;
    DeferredPartsSync & operator=(const DeferredPartsSync &) = delete;

    /// If the insert fails or is cancelled, the parts that it has already committed stay in the table.
    /// They are synced here, so that they are not left without fsync on any exit path.
    ~DeferredPartsSync();

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

    OnFailedSync on_failed_sync;
    std::deque<PartToSync> parts;
    std::unordered_set<const IMergeTreeDataPart *> queued_parts;

    void sync(bool only_old);
};

}
