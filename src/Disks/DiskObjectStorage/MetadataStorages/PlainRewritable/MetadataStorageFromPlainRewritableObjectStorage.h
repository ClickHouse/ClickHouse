#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PlainRewritableSnapshotFile.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/PathLocks.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Common/Logger.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/** Stores data in immutable files, but allows atomic directory renames and hard links, which is suitable for MergeTree tables.
  *
  * The structure in object storage is as follows:
  * - every directory, regardless of its name and depth, is stored in a randomly-named directory at root;
  * - files inside the directory are stored as is;
  * - additionally, there is metadata, which contains mappings from random names to actual, logical paths;
  * - this directory (named `__meta` and located at root) contains subdirectories with the corresponding random name,
  *   each containing a single file, `prefix.path`, with the content as the logical path of the corresponding directory.
  * - when a logical directory is renamed or moved, we don't touch its randomly assigned name,
  *   and simply rewrite the contents of `prefix.path`.
  * - a removal is committed by moving the directory (or a backup copy of the file) under a logical name
  *   starting with `__removed.`, and the objects are deleted afterwards; if the process dies in between,
  *   such objects are deleted on the next initial load, see `PlainRewritableLayout::REMOVED_NAME_PREFIX`.
  *
  * Example. Let's suppose, the logical filesystem structure is:
  * /hello/world/test1.txt
  * /test2.txt
  *
  * The physical structure will be:
  * /__meta/aaealinyzgdzycgcnpgaapdssrjirnnr/prefix.path, contents: /
  * /__meta/gfkoqxvyhaasroiodbeurnftnwieiihy/prefix.path, contents: /hello/world/
  * /__meta/xelohvynszqqinrvcygwzpdwvsklbxkk/prefix.path, contents: /hello/
  * /aaealinyzgdzycgcnpgaapdssrjirnnr/test2.txt
  * /gfkoqxvyhaasroiodbeurnftnwieiihy/test1.txt
  *
  * The `prefix.path` above is in the implicit form: the files of the directory are whatever blobs are stored under its prefix.
  * To support hard links, a directory can be switched to the explicit form, where `prefix.path` also lists the files
  * with the keys of their blobs (see `PrefixPath.h`), so a file can point to a blob under the prefix of another directory:
  *
  * /__meta/gfkoqxvyhaasroiodbeurnftnwieiihy/prefix.path, contents:
  *     /hello/world/
  *     files: 2
  *     link.txt    aaealinyzgdzycgcnpgaapdssrjirnnr/test2.txt    42
  *     test1.txt   gfkoqxvyhaasroiodbeurnftnwieiihy/test1.txt    7
  *
  * The number of links to every blob is maintained in memory (and recalculated on load): a blob is removed together with its last link.
  * New blobs of a directory in the explicit form get random names, so that a new file cannot clobber the blob of a deleted file
  * that is still linked from elsewhere. Directories that never had hard links stay in the implicit form, so the layout of a disk
  * without hard links is unchanged.
  *
  * The explicit form cannot be read by servers older than the version that introduced it, so a disk that has it is not readable
  * after a downgrade. Therefore the creation of hard links can be disabled by the `enable_hard_links` setting of the disk
  * (on by default); with hard links disabled, `createHardLink` copies the blob, as it did before, and no directory ever
  * switches to the explicit form. The explicit form is always *read*, so a disk written with hard links enabled stays usable
  * after they are disabled again.
  *
  * Additionally, `/__meta/snapshot.bin` may contain a compact copy of the whole state (see `PlainRewritableSnapshotFile.h`),
  * which is written by the disk that owns the data and allows read-only disks to load the state in a single request instead
  * of listing the metadata directory and reading every `prefix.path`.
  */

struct PlainRewritableSnapshotSettings
{
    /// Whether to write the snapshot file after modifications (if the object storage is writable),
    /// or to load the state from it instead of listing the object storage (if the object storage is read-only).
    bool enabled = true;
    /// Zero means the snapshot is rewritten right after every modification (in the committing thread).
    /// A larger value means it is rewritten in the background not more often than the delay, as long as there were modifications.
    /// The whole state is serialized on every write, so writing it in the committing thread would make the latency of
    /// every commit proportional to the size of the whole disk; that is why it is delayed by default.
    UInt64 write_delay_ms = 1000;
};

class MetadataStorageFromPlainRewritableObjectStorage final : public IMetadataStorage
{
    friend class MetadataStorageFromPlainRewritableObjectStorageTransaction;

    enum class LoadMode
    {
        /// The first load in the constructor.
        Initial,
        /// Periodic `refresh`: pick up the changes made by another server, keeping the unchanged directories.
        Incremental,
        /// `dropCache`: rebuild the state from scratch.
        Full,
    };

    /// Must be called under `load_mutex`.
    void load(LoadMode mode);

    struct SnapshotFileContents
    {
        /// The file exists (but `layout` is empty if it could not be read or did not change).
        bool exists = false;
        /// The file has the same ETag as the one applied last time, so it was not read.
        bool unchanged = false;
        std::optional<PlainRewritableRemoteLayout> layout;
        /// The strong ETag of the file (also if it was not read), empty if there is none.
        std::string etag;
    };

    /// Reads the snapshot file if it exists, unless `skip_if_unchanged` and it is the same as the one applied last time,
    /// or it is the same as `superseded_snapshot_etag`, or not `read_contents` (then only `exists` and `etag` are filled).
    /// Must be called under `load_mutex`.
    SnapshotFileContents tryReadSnapshotFile(const LoggerPtr & log, bool skip_if_unchanged, bool read_contents) const;

    struct TombstoneMarkerObject
    {
        std::string removed_name;
        std::string marker_path;
        std::string content;
    };

    /// Lists and reads the markers of unfinished operations (see `PlainRewritableLayout::REMOVED_NAME_PREFIX`).
    /// A marker that disappears in the meantime is skipped.
    std::vector<TombstoneMarkerObject> readTombstoneMarkers(const ReadSettings & settings) const;

    struct TargetReadFromBackup
    {
        PlainRewritableLayout::PendingReplace pending_replace;
        std::string backup_key;
        ObjectMetadata backup_metadata;
    };

    /// The targets of the replacements that are not committed yet, which a read-only disk reads from their complete backups.
    /// Sets `has_malformed_markers` if there is a marker whose content is torn (see `PlainRewritableLayout::parseTombstoneMarkerContent`).
    std::vector<TargetReadFromBackup> findTargetsToReadFromBackups(const LoggerPtr & log, bool & has_malformed_markers) const;

    /// Lists the object storage and publishes the result in `fs`. If `base` is provided, directories whose `prefix.path`
    /// has the same ETag as in `base` are taken from it without reading `prefix.path` and listing their files.
    /// Returns whether the disk has nothing but the empty root. The initial load of a writable disk also rolls back or
    /// reclaims the unfinished operations (see `PlainRewritableLayout::REMOVED_NAME_PREFIX`). Must be called under `load_mutex`.
    bool loadRemoteLayout(const PlainRewritableRemoteLayout * base, bool is_initial_load, const LoggerPtr & log);
    PlainRewritableRemoteLayout getCurrentLayout() const;
    static PlainRewritableRemoteLayout getLayoutOf(const FsSnapshot & tree);

    /// Whether the disk does not write to the object storage: either the object storage is read-only, or the disk is
    /// attached with `read_only` (wrapped with `ReadOnlyDiskWrapper`), which does not make the object storage read-only.
    bool isReadOnlyDisk() const { return read_only_disk || object_storage->isReadOnly(); }

    /// The role of the disk regarding the snapshot file. It is chosen once when the disk is created: the task that writes
    /// the snapshot exists only for a writer, and a reader must never publish its own state over the one of the writer.
    /// Changing `readonly` of the object storage by `SYSTEM RELOAD CONFIG` does not change it, that requires a restart.
    enum class SnapshotRole : uint8_t
    {
        None,
        /// Snapshots are enabled and the disk is writable.
        Writer,
        /// Snapshots are enabled and the disk is read-only.
        Reader,
    };
    SnapshotRole chooseSnapshotRole() const;
    bool isSnapshotWriter() const { return snapshot_role == SnapshotRole::Writer; }
    bool isSnapshotReader() const { return snapshot_role == SnapshotRole::Reader; }
    /// Called after the in-memory state was changed; writes the snapshot right away or schedules the write.
    void onLayoutChanged();
    /// Writes the current state to the snapshot file (or removes the file if the disk is empty) if there were changes.
    /// Only one write is performed even if there were several changes, since the latest state is written.
    /// If `required_generation` is non-zero, nothing is done if a previous write already included the change of this generation.
    void writeSnapshotIfDirty(UInt64 required_generation = 0);
    void snapshotWriteTask();

public:
    MetadataStorageFromPlainRewritableObjectStorage(
        ObjectStoragePtr object_storage_,
        std::string storage_path_prefix_,
        bool hard_links_enabled_,
        PlainRewritableSnapshotSettings snapshot_settings_ = {},
        bool read_only_disk_ = false);

    MetadataStorageType getType() const override { return MetadataStorageType::PlainRewritable; }
    const std::string & getPath() const override { return storage_path_full; }
    uint32_t getHardlinkCount(const std::string & path) const override;
    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return false; }
    bool isRemote() const override { return object_storage->isRemote(); }
    bool areBlobPathsRandom() const override { return false; }
    bool isPlain() const override { return true; }
    bool isWriteOnce() const override { return false; }
    /// Blobs are removed right in the transaction, and there is nothing to replicate.
    bool hasDeadBlobsQueue() const override { return false; }
    bool hasMissingBlobsQueue() const override { return false; }
    bool supportsHardLinks() const override { return hard_links_enabled; }

    MetadataTransactionPtr createTransaction() override;

    /// Will reload in-memory structure from scratch.
    void dropCache() override;
    void refresh(UInt64 not_sooner_than_milliseconds) override;
    void shutdown() override;

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const std::string & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;
    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;
    std::optional<Poco::Timestamp> getLastModifiedIfExists(const std::string & path) const override;

private:
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const std::string storage_path_prefix;
    const std::string storage_path_full;
    /// Real hard links require the explicit form of `prefix.path`, which older servers cannot read,
    /// so they can be disabled by the `enable_hard_links` setting of the disk. See the comment above.
    /// The setting is startup-only (`applyNewSettings` is not overridden): `DiskObjectStorage::applyNewSettings` passes the
    /// config prefix of the outer disk, which is not the prefix of this one when the disk is wrapped with a cache, so a reload
    /// would read the setting from the wrong place. Changing it requires a restart, as a downgrade does anyway.
    const bool hard_links_enabled;
    const PlainRewritableSnapshotSettings snapshot_settings;
    /// The disk is attached with `read_only`, see `isReadOnlyDisk`.
    const bool read_only_disk;
    const SnapshotRole snapshot_role;

    /// Transactions hold the locks for the paths they modify while they talk to the object storage and publish the result;
    /// full reloads of the metadata hold the lock for the root. Transactions on unrelated paths run concurrently.
    PathLocks path_locks;
    FsMetadata fs;
    std::shared_ptr<PlainRewritableLayout> layout;

    std::mutex load_mutex;
    /// The remote paths of the directories of the targets of the pending replacements seen by the last completed load.
    /// Protected by `load_mutex`.
    std::unordered_set<std::string> remote_directories_of_pending_replaces;
    /// The ETag of the snapshot file applied by the last load of a reader, empty if the state was not loaded from it.
    /// Protected by `load_mutex`.
    std::string applied_snapshot_etag;
    /// Some files of the state loaded from the snapshot file are read from the backups of pending replacements.
    /// Protected by `load_mutex`.
    bool applied_snapshot_had_pending_replaces = false;
    /// The current state was loaded from the snapshot file (even if its ETag is not usable and `applied_snapshot_etag` is
    /// empty), so it cannot be a base of an incremental listing. Protected by `load_mutex`.
    bool state_loaded_from_snapshot = false;
    /// The ETag of the snapshot file that existed when a reader loaded the state by listing the object storage instead
    /// (e.g. the file could not be read), empty if there is none. That state is at least as fresh as the file, so the file
    /// is not applied until it changes. Protected by `load_mutex`.
    std::string superseded_snapshot_etag;
    AtomicStopwatch previous_refresh;

    /// The version of `fs` after the latest change that has to be written to the snapshot.
    std::atomic<UInt64> snapshot_dirty_generation = 0;
    /// Serializes the snapshot writes.
    std::mutex snapshot_write_mutex;
    /// The version of `fs` in the last successfully written snapshot. The snapshot is dirty while it is less than
    /// `snapshot_dirty_generation`. Modified under `snapshot_write_mutex`.
    std::atomic<UInt64> snapshot_written_generation = 0;
    /// Must be the last member: it is deactivated first in the destructor, and the task uses the other members.
    BackgroundSchedulePoolTaskHolder snapshot_write_task;
};

class MetadataStorageFromPlainObjectStorageCopyFileOperation;

class MetadataStorageFromPlainRewritableObjectStorageTransaction : public IMetadataTransaction
{
protected:
    MetadataStorageFromPlainRewritableObjectStorage & metadata_storage;

    std::shared_ptr<FsSnapshot> commit_snapshot;
    UncommittedState uncommitted_state;
    MetadataOperationsHolder operations;
    StoredObjects removed_objects;
    /// Blob keys chosen by `generateObjectKeyForPath`, by normalized file path, for the files this transaction is going to create.
    std::unordered_map<std::string, std::string> generated_blob_keys;
    /// The copies that stand in for hard links while hard links are disabled, by the normalized path of the target.
    /// A rewrite of the target in the same transaction supersedes its copy. Once the target is moved or replaced, both the
    /// old and the new path are marked as `moved`, and a rewrite of them is refused.
    struct FallbackCopy
    {
        MetadataStorageFromPlainObjectStorageCopyFileOperation * copy = nullptr;
        bool moved = false;
    };
    std::unordered_map<std::string, FallbackCopy> fallback_copies;

    void planFileMove(const NormalizedPath & path_from, const NormalizedPath & path_to);
    void markFallbackCopyMoved(const std::string & path_from, const std::string & path_to);

    /// Normalized paths of the files and directories the operations modify; locked for the duration of the commit.
    std::vector<std::string> affected_paths;

    void addAffectedPath(const std::string & path);
    /// The same for a file, and also for its directory if the operation may rewrite the `prefix.path` of the directory.
    void addAffectedFilePath(const std::string & path);

public:
    explicit MetadataStorageFromPlainRewritableObjectStorageTransaction(MetadataStorageFromPlainRewritableObjectStorage & metadata_storage_);

    bool supportsChmod() const override { return false; }
    void setLastModified(const String &, const Poco::Timestamp &) override { /* Noop */ }
    void setReadOnly(const std::string & /*path*/) override { /* Noop */ }

    void commit(const TransactionCommitOptionsVariant & options) override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;

    void createMetadataFile(const std::string & /* path */, const StoredObjects & /* objects */) override;
    void createDirectory(const std::string & path) override;
    void createDirectoryRecursive(const std::string & path) override;
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;
    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    StoredObjects getSubmittedForRemovalBlobs() override;
};

}
