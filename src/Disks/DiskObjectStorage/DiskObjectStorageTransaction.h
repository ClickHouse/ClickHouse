#pragma once

#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/IDiskTransaction.h>

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
protected:
    const ClusterConfigurationPtr cluster;
    const MetadataStoragePtr metadata_storage;
    const ObjectStorageRouterPtr object_storages;

    MetadataTransactionPtr metadata_transaction;
    std::vector<std::function<void(MetadataTransactionPtr tx)>> operations_to_execute;
    std::unordered_map<Location, StoredObjects> written_blobs;

public:
    DiskObjectStorageTransaction(
        ClusterConfigurationPtr cluster_,
        MetadataStoragePtr metadata_storage_,
        ObjectStorageRouterPtr object_storages_);

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

private:
    std::unique_ptr<WriteBufferFromFileBase> writeFileImpl( /// NOLINT
        bool autocommit,
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings);
};

/// Only needed for S3 server side object copy
struct MultipleDisksObjectStorageTransaction final : public DiskObjectStorageTransaction, std::enable_shared_from_this<MultipleDisksObjectStorageTransaction>
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
        ObjectStorageRouterPtr destination_object_storages_);

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings &) override;
};

using DiskObjectStorageTransactionPtr = std::shared_ptr<DiskObjectStorageTransaction>;

}
