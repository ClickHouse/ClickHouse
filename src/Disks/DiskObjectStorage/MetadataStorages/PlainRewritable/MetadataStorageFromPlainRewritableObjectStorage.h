#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <memory>

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
  * after a downgrade. Therefore the creation of hard links is off by default and has to be enabled by the `enable_hard_links`
  * setting of the disk; with hard links disabled, `createHardLink` copies the blob, as it did before, and no directory ever
  * switches to the explicit form. The explicit form is always *read*, so a disk written with hard links enabled stays usable
  * after they are disabled again.
  */
class MetadataStorageFromPlainRewritableObjectStorage final : public IMetadataStorage
{
    friend class MetadataStorageFromPlainRewritableObjectStorageTransaction;

    void load(bool is_initial_load, bool do_not_load_unchanged_directories);

public:
    MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, std::string storage_path_prefix_, bool hard_links_enabled_);

    MetadataStorageType getType() const override { return MetadataStorageType::PlainRewritable; }
    const std::string & getPath() const override { return storage_path_full; }
    uint32_t getHardlinkCount(const std::string & path) const override;
    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return false; }
    bool areBlobPathsRandom() const override { return false; }
    bool isPlain() const override { return true; }
    bool isWriteOnce() const override { return false; }
    bool supportsHardLinks() const override { return hard_links_enabled; }
    /// Blobs are removed right in the transaction, and there is nothing to replicate.
    bool hasDeadBlobsQueue() const override { return false; }
    bool hasMissingBlobsQueue() const override { return false; }

    MetadataTransactionPtr createTransaction() override;

    /// Will reload in-memory structure from scratch.
    void dropCache() override;
    void refresh(UInt64 not_sooner_than_milliseconds) override;

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
    /// so they are enabled by the `enable_hard_links` setting of the disk. See the comment above.
    const bool hard_links_enabled;

    std::mutex metadata_mutex;
    FsMetadata fs;
    std::shared_ptr<PlainRewritableLayout> layout;

    std::mutex load_mutex;
    AtomicStopwatch previous_refresh;
};

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

    void planFileMove(const NormalizedPath & path_from, const NormalizedPath & path_to);

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
