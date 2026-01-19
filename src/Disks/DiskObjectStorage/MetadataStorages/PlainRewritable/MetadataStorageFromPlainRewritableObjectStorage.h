#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>

#include <memory>

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
  */
class MetadataStorageFromPlainRewritableObjectStorage final : public IMetadataStorage
{
    friend class MetadataStorageFromPlainRewritableObjectStorageTransaction;

    void load(bool is_initial_load, bool do_not_load_unchanged_directories);

public:
    MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, std::string storage_path_prefix_);

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

    std::mutex metadata_mutex;
    std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    std::shared_ptr<PlainRewritableLayout> layout;

    std::mutex load_mutex;
    AtomicStopwatch previous_refresh;
};

class MetadataStorageFromPlainRewritableObjectStorageTransaction : public IMetadataTransaction
{
protected:
    MetadataStorageFromPlainRewritableObjectStorage & metadata_storage;

    /// Plain rewritable disks extract key names for files from generated directory keys. Here we will
    /// maintain uncommitted directory tree that was populated during metadata transaction filling to be able
    /// to extrace remote path of directory during nested file creation in the same transaction.
    std::shared_ptr<InMemoryDirectoryTree> uncommitted_fs_tree;
    MetadataOperationsHolder operations;

public:
    explicit MetadataStorageFromPlainRewritableObjectStorageTransaction(MetadataStorageFromPlainRewritableObjectStorage & metadata_storage_);

    bool supportsChmod() const override { return false; }
    void setLastModified(const String &, const Poco::Timestamp &) override { /* Noop */ }
    void setReadOnly(const std::string & /*path*/) override { /* Noop */ }

    void commit(const TransactionCommitOptionsVariant & options) override;

    void createMetadataFile(const std::string & /* path */, const StoredObjects & /* objects */) override;
    void createDirectory(const std::string & path) override;
    void createDirectoryRecursive(const std::string & path) override;
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string &) override;

    /// Hard links are simulated using server-side copying.
    void createHardLink(const std::string & path_from, const std::string & path_to) override;
    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    const IMetadataStorage & getStorageForNonTransactionalReads() const override;
    std::optional<StoredObjects> tryGetBlobsFromTransactionIfExists(const std::string & path) const override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
};

}
