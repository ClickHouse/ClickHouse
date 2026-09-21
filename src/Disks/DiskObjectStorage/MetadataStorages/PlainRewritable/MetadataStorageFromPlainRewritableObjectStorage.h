#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PlainRewritableSnapshotFile.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Common/Logger.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace DB
{

/** Stores data in immutable files, but allows atomic directory renames, which is suitable for MergeTree tables.
  *
  * The structure in object storage is as follows:
  * - every directory, regardless of its name and depth, is stored in a randomly-named directory at root;
  * - files inside the directory are stored as is;
  * - additionally, there is metadata, which contains mappings from random names to actual, logical paths;
  * - this directory (named `__meta` and located at root) contains subdirectories with the corresponding random name,
  *   each containing a single file, `prefix.path`, with the content as the logical path of the corresponding directory.
  * - when a logical directory is renamed or moved, we don't touch its randomly assigned name,
  *   and simply rewrite the contents of `prefix.path`.
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
  * Additionally, `/__meta/snapshot.bin` may contain a compact copy of the whole state (see `PlainRewritableSnapshotFile.h`),
  * which is written by the disk that owns the data and allows loading the state in a single request instead of
  * listing the metadata directory and reading every `prefix.path`.
  */

struct PlainRewritableSnapshotSettings
{
    /// Whether to write the snapshot file after modifications (if the object storage is writable)
    /// and to prefer it over listing the object storage when loading the state.
    bool enabled = true;
    /// Zero means the snapshot is rewritten right after every modification (in the committing thread).
    /// A larger value means it is rewritten in the background not more often than the delay, as long as there were modifications.
    UInt64 write_delay_ms = 0;
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
        /// `dropCache`: rebuild the state from the object storage listing, ignoring the snapshot file.
        Full,
    };

    /// Must be called under `load_mutex`.
    void load(LoadMode mode);

    struct SnapshotFileContents
    {
        /// The file exists (but `layout` is empty if it could not be read or was not read).
        bool exists = false;
        /// The file has the ETag that was asked to skip, so it was not read.
        bool unchanged = false;
        std::optional<PlainRewritableRemoteLayout> layout;
        std::string etag;
    };

    /// Reads the snapshot file if it exists, unless its ETag is `skip_if_etag`.
    SnapshotFileContents tryReadSnapshotFile(const LoggerPtr & log, const std::optional<std::string> & skip_if_etag) const;

    /// Lists the object storage. If `base` is provided, directories whose `prefix.path` has the same ETag as in `base`
    /// are taken from it without reading. `differs_from_base` tells whether the result is different from `base`.
    PlainRewritableRemoteLayout listRemoteLayout(const PlainRewritableRemoteLayout * base, bool & differs_from_base, const LoggerPtr & log) const;
    PlainRewritableRemoteLayout getCurrentLayout() const;

    /// Whether this disk writes the snapshot file: snapshots are enabled and the object storage is writable.
    bool isSnapshotWriter() const;
    /// Called after the in-memory state was changed; writes the snapshot right away or schedules the write.
    void onLayoutChanged();
    /// Writes the current state to the snapshot file (or removes the file if the disk is empty) if there were changes.
    /// Only one write is performed even if there were several changes, since the latest state is written.
    void writeSnapshotIfDirty();
    void snapshotWriteTask();

public:
    MetadataStorageFromPlainRewritableObjectStorage(
        ObjectStoragePtr object_storage_,
        std::string storage_path_prefix_,
        PlainRewritableSnapshotSettings snapshot_settings_ = {});

    MetadataStorageType getType() const override { return MetadataStorageType::PlainRewritable; }
    const std::string & getPath() const override { return storage_path_full; }
    uint32_t getHardlinkCount(const std::string & /* path */) const override { return 0; }
    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return false; }
    bool areBlobPathsRandom() const override { return false; }
    bool isPlain() const override { return true; }
    bool isWriteOnce() const override { return false; }

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
    const PlainRewritableSnapshotSettings snapshot_settings;

    std::mutex metadata_mutex;
    FsMetadata fs;
    std::shared_ptr<PlainRewritableLayout> layout;

    std::mutex load_mutex;
    AtomicStopwatch previous_refresh;
    /// ETag of the snapshot file the state was loaded from; a refresh is skipped if the file did not change.
    /// Guarded by `load_mutex`.
    std::string loaded_snapshot_etag;

    /// Set after every change of the state, cleared when the snapshot write starts.
    std::atomic<bool> snapshot_dirty = false;
    /// Serializes the snapshot writes.
    std::mutex snapshot_write_mutex;
    /// Must be the last member: it is deactivated first in the destructor, and the task uses the other members.
    BackgroundSchedulePoolTaskHolder snapshot_write_task;
};

class MetadataStorageFromPlainRewritableObjectStorageTransaction : public IMetadataTransaction
{
protected:
    MetadataStorageFromPlainRewritableObjectStorage & metadata_storage;

    std::shared_ptr<FsSnapshot> commit_snapshot;
    UncommittedState uncommitted_state;
    MetadataOperationsHolder operations;
    StoredObjects removed_objects;

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

    /// Hard links are simulated using server-side copying.
    void createHardLink(const std::string & path_from, const std::string & path_to) override;
    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    StoredObjects getSubmittedForRemovalBlobs() override;
};

}
