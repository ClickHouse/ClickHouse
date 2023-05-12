#pragma once

#include <Disks/IDiskTransaction.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>

namespace DB
{


/// Basic operation inside disk object storage transaction.
struct IDiskObjectStorageOperation
{
    /// useful for operations with blobs in object storage
    IObjectStorage & object_storage;
    /// useful for some read operations
    IMetadataStorage & metadata_storage;
public:
    IDiskObjectStorageOperation(IObjectStorage & object_storage_, IMetadataStorage & metadata_storage_)
        : object_storage(object_storage_)
        , metadata_storage(metadata_storage_)
    {}

    /// Execute operation and something to metadata transaction
    virtual void execute(MetadataTransactionPtr transaction) = 0;
    /// Revert operation if possible
    virtual void undo() = 0;
    /// Action to execute after metadata transaction successfully committed.
    /// Useful when it's impossible to revert operation
    /// like removal of blobs. Such implementation can lead to garbage.
    virtual void finalize() = 0;
    virtual ~IDiskObjectStorageOperation() = default;

    virtual std::string getInfoForLog() const = 0;
};

using DiskObjectStorageOperation = std::unique_ptr<IDiskObjectStorageOperation>;

using DiskObjectStorageOperations = std::vector<DiskObjectStorageOperation>;


/// Disk object storage transaction, actually implement some part of disk object storage
/// logic. Works on top of non atomic operations with blobs and possibly atomic implementation
/// of metadata storage.
///
/// Commit works like:
/// 1. Execute all accumulated operations in loop.
/// 2. Commit metadata transaction.
/// 3. Finalize all accumulated operations in loop.
///
/// If something wrong happen on step 1 or 2 reverts all applied operations.
/// If finalize failed -- nothing is reverted, garbage is left in blob storage.
struct DiskObjectStorageTransaction final : public IDiskTransaction, std::enable_shared_from_this<DiskObjectStorageTransaction>
{
private:
    IObjectStorage & object_storage;
    IMetadataStorage & metadata_storage;

    MetadataTransactionPtr metadata_transaction;

    /// TODO we can get rid of this params
    DiskObjectStorageRemoteMetadataRestoreHelper * metadata_helper;

    DiskObjectStorageOperations operations_to_execute;

public:
    DiskObjectStorageTransaction(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        DiskObjectStorageRemoteMetadataRestoreHelper * metadata_helper_);

    void commit() override;

    void createDirectory(const std::string & path) override;

    void createDirectories(const std::string & path) override;

    void clearDirectory(const std::string & path) override;

    void moveDirectory(const std::string & from_path, const std::string & to_path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const std::string & from_path, const std::string & to_path) override;

    void createFile(const String & path) override;

    void copyFile(const std::string & from_file_path, const std::string & to_file_path) override;

    /// writeFile is a difficult function for transactions.
    /// Now it's almost noop because metadata added to transaction in finalize method
    /// of write buffer. Autocommit means that transaction will be immediately committed
    /// after returned buffer will be finalized.
    std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const std::string & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {},
        bool autocommit = true) override;

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
};

using DiskObjectStorageTransactionPtr = std::shared_ptr<DiskObjectStorageTransaction>;

}
