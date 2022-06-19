#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>

namespace DB
{


struct IMetadataOperation
{
    virtual void execute() = 0;
    virtual void undo() = 0;
    virtual void finalize() {}
    virtual ~IMetadataOperation() = default;
};

using MetadataOperationPtr = std::unique_ptr<IMetadataOperation>;

enum class MetadataFromDiskTransactionState
{
    PREPARING,
    FAILED,
    COMMITTED,
    PARTIALLY_ROLLED_BACK,
};

std::string toString(MetadataFromDiskTransactionState state);

class MetadataStorageFromDisk final : public IMetadataStorage
{
private:
    friend struct MetadataStorageFromDiskTransaction;

    DiskPtr disk;
    std::string root_path_for_remote_metadata;
    mutable std::shared_mutex metadata_mutex;

public:
    MetadataStorageFromDisk(DiskPtr disk_, const std::string & root_path_from_remote_metadata_)
        : disk(disk_)
        , root_path_for_remote_metadata(root_path_from_remote_metadata_)
    {
    }

    MetadataTransactionPtr createTransaction() const override;

    const std::string & getPath() const override;

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    time_t getLastChanged(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    BlobsPathToSize getBlobs(const std::string & path) const override;

    std::vector<std::string> getRemotePaths(const std::string & path) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;


private:
    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> & lock) const;
};

struct MetadataStorageFromDiskTransaction final : public IMetadataTransaction
{
private:
    const MetadataStorageFromDisk & metadata_storage;

    std::vector<MetadataOperationPtr> operations;
    MetadataFromDiskTransactionState state{MetadataFromDiskTransactionState::PREPARING};

    void addOperation(MetadataOperationPtr && operation);
    void rollback(size_t until_pos);

public:
    explicit MetadataStorageFromDiskTransaction(const MetadataStorageFromDisk & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    const IMetadataStorage & getStorageForNonTransactionalReads() const override
    {
        return metadata_storage;
    }

    void commit() override;

    void writeStringToFile(const std::string & path, const std::string & data) override;

    void createEmptyMetadataFile(const std::string & path) override;

    void createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) override;

    void addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    void setReadOnly(const std::string & path) override;

    void unlinkFile(const std::string & path) override;

    void createDirectory(const std::string & path) override;

    void createDicrectoryRecursive(const std::string & path) override;

    void removeDirectory(const std::string & path) override;

    void removeRecursive(const std::string & path) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;

    void moveFile(const std::string & path_from, const std::string & path_to) override;

    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    void unlinkMetadata(const std::string & path) override;

    ~MetadataStorageFromDiskTransaction() override = default;
};


}
