#pragma once

#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/IDiskTransaction.h>

#include <Common/ThreadPool_fwd.h>

#include <memory>

namespace DB
{

/// Disk object storage transaction, actually implement some part of disk object storage
/// logic. Works on top of non atomic operations with blobs and possibly atomic implementation
/// of metadata storage.
///
/// Commit works like:
/// 1. Execute all accumulated operations in loop.
/// 2. Commit metadata transaction.
struct DiskObjectStorageTransaction : public IDiskTransaction, public std::enable_shared_from_this<DiskObjectStorageTransaction>
{
    void waitBlobRemoval(const StoredObjects & blobs) const;

protected:
    const ClusterConfigurationPtr cluster;
    const MetadataStoragePtr metadata_storage;
    const ObjectStorageRouterPtr object_storages;
    const BlobKillerThreadPtr blob_killer;
    /// Thread pool used by `copyFile` to dispatch `copyObjectToAnotherObjectStorage`
    /// calls in parallel. Owned by `DiskObjectStorage` and shared across transactions.
    const std::shared_ptr<ThreadPool> copy_object_pool;
    const bool wait_blob_removal;
    const std::string read_resource_name;
    const std::string write_resource_name;

    MetadataTransactionPtr metadata_transaction;
    std::vector<std::function<void(MetadataTransactionPtr tx)>> operations_to_execute;
    std::unordered_map<Location, StoredObjects> written_blobs;

public:
    DiskObjectStorageTransaction(
        ClusterConfigurationPtr cluster_,
        MetadataStoragePtr metadata_storage_,
        ObjectStorageRouterPtr object_storages_,
        BlobKillerThreadPtr blob_killer_,
        std::shared_ptr<ThreadPool> copy_object_pool_,
        bool wait_blob_removal_,
        std::string read_resource_name_,
        std::string write_resource_name_);

    void commit() override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;
    void undo() noexcept override;

    void createDirectory(const std::string & path) override;

    void createDirectories(const std::string & path) override;

    void moveDirectory(const std::string & from_path, const std::string & to_path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const std::string & from_path, const std::string & to_path) override;

    void createFile(const String & path) override;

    void truncateFile(const String & path, size_t size) override;

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings &) override;

    /// writeFile is a difficult function for transactions.
    /// Now it's almost noop because metadata added to transaction in finalize method
    /// of write buffer. Autocommit means that transaction will be immediately committed
    /// after returned buffer will be finalized.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;
    std::unique_ptr<WriteBufferFromFileBase> writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    /// Write a file using a custom function to write an object to the disk's object storage.
    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override;

    void removeFile(const std::string & path) override;
    void removeFileIfExists(const std::string & path) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path) override;

    void removeSharedFile(const std::string & path, bool keep_shared_data) override;
    void removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only) override;
    void removeSharedFileIfExists(const std::string & path, bool keep_shared_data) override;
    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;
    void chmod(const String & path, mode_t mode) override;
    void setReadOnly(const std::string & path) override;
    void createHardLink(const std::string & src_path, const std::string & dst_path) override;

    /// B59 in-flight read-your-writes: forward to the metadata transaction (e.g. a CA part-build
    /// transaction resolving its own staged-but-uncommitted files).
    std::optional<StoredObjects> tryGetInFlightStorageObjects(const std::string & path) const override;
    std::unique_ptr<ReadBufferFromFileBase> tryReadFileInFlight(
        const std::string & path, const ReadSettings & settings, std::optional<size_t> read_hint) const override;
    std::optional<uint64_t> tryGetInFlightFileSize(const std::string & path) const override;
    bool hasInFlightDirectory(const std::string & path) const override;
    std::vector<std::string> listInFlightDirectory(const std::string & path) const override;

protected:
    /// Shared between `DiskObjectStorageTransaction::copyFile` and
    /// `MultipleDisksObjectStorageTransaction::copyFile`. Reads source blobs from the
    /// passed-in source triple and writes them onto this transaction's destination
    /// (`metadata_transaction`, `object_storages`, `written_blobs`, `operations_to_execute`).
    void copyFileImpl(
        const MetadataStoragePtr & src_metadata_storage,
        const ClusterConfigurationPtr & src_cluster,
        const ObjectStorageRouterPtr & src_object_storages,
        const std::string & from_file_path,
        const std::string & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings);

    /// [TXN-ONE-PIPELINE] Route one metadata effect either into the FIFO replay queue (ordinary object
    /// storage) or straight to the metadata transaction at call time (eager staging overlay, e.g. CA).
    template <typename Operation>
    void dispatch(Operation && operation)
    {
        if (metadata_storage->transactionIsStagingOverlay())
            operation(metadata_transaction);
        else
            operations_to_execute.emplace_back(std::forward<Operation>(operation));
    }

private:
    std::unique_ptr<WriteBufferFromFileBase> writeFileImpl( /// NOLINT
        bool autocommit,
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings);
};

/// Only needed for S3 server side object copy
struct MultipleDisksObjectStorageTransaction final : public DiskObjectStorageTransaction
{
    ClusterConfigurationPtr source_cluster;
    MetadataStoragePtr source_metadata_storage;
    ObjectStorageRouterPtr source_object_storages;

    MultipleDisksObjectStorageTransaction(
        ClusterConfigurationPtr source_cluster_,
        MetadataStoragePtr source_metadata_storage_,
        ObjectStorageRouterPtr source_object_storages_,
        ClusterConfigurationPtr destination_cluster_,
        MetadataStoragePtr destination_metadata_storage_,
        ObjectStorageRouterPtr destination_object_storages_,
        std::shared_ptr<ThreadPool> copy_object_pool_,
        std::string read_resource_name_,
        std::string write_resource_name_);

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings &) override;
};

using DiskObjectStorageTransactionPtr = std::shared_ptr<DiskObjectStorageTransaction>;

}
