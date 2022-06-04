#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>

namespace DB
{

enum class MetadataFromDiskTransactionState
{
    PREPARING,
    FAILED,
    COMMITTED,
    ROLLED_BACK,
    PARTIALLY_ROLLED_BACK,
};

std::string toString(MetadataFromDiskTransactionState state);

struct MetadataStorageFromDiskTransaction final : public IMetadataTransaction
{
private:
    std::optional<size_t> failed_operation_index;

    std::shared_mutex & commit_mutex;
    std::vector<MetadataOperationPtr> operations;
    MetadataFromDiskTransactionState state{MetadataFromDiskTransactionState::PREPARING};
public:
    explicit MetadataStorageFromDiskTransaction(std::shared_mutex & commit_mutex_)
        : commit_mutex(commit_mutex_)
    {}

    void addOperation(MetadataOperationPtr && operation) override;
    void commit() override;
    void rollback() override;

    ~MetadataStorageFromDiskTransaction() override;
};

class MetadataStorageFromDisk final : public IMetadataStorage
{
private:
    DiskPtr disk;
    std::string root_path_for_remote_metadata;
    mutable std::shared_mutex metadata_mutex;

public:
    explicit MetadataStorageFromDisk(DiskPtr disk_, const std::string & root_path_from_remote_metadata_ = "")
        : disk(disk_)
        , root_path_for_remote_metadata(root_path_from_remote_metadata_)
    {
    }

    MetadataTransactionPtr createTransaction() const override
    {
        return std::make_shared<MetadataStorageFromDiskTransaction>(metadata_mutex);
    }

    const std::string & getPath() const override;

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(  /// NOLINT
         const std::string & path,
         const ReadSettings & settings = ReadSettings{},
         std::optional<size_t> read_hint = {},
         std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
         const std::string & path,
         MetadataTransactionPtr transaction,
         size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
         const WriteSettings & settings = {}) override;

    void createMetadataFile(const std::string & path, MetadataTransactionPtr transaction) override;

    void addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes, MetadataTransactionPtr transaction) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp, MetadataTransactionPtr transaction) override;

    void setReadOnly(const std::string & path, MetadataTransactionPtr transaction) override;

    void unlinkFile(const std::string & path, MetadataTransactionPtr transaction) override;

    void createDirectory(const std::string & path, MetadataTransactionPtr transaction) override;

    void createDicrectoryRecursive(const std::string & path, MetadataTransactionPtr transaction) override;

    void removeDirectory(const std::string & path, MetadataTransactionPtr transaction) override;

    void removeRecursive(const std::string & path, MetadataTransactionPtr transaction) override;

    void createHardLink(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void moveFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void moveDirectory(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void replaceFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    BlobsPathToSize getBlobs(const std::string & path) const override;

    std::vector<std::string> getRemotePaths(const std::string & path) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    uint32_t unlinkAndGetHardlinkCount(const std::string & path, MetadataTransactionPtr transaction) override;

private:

    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> & lock) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::unique_lock<std::shared_mutex> & lock) const;
};


}
