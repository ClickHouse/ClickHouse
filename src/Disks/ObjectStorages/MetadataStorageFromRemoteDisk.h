#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransaction.h>

namespace DB
{

class MetadataStorageFromRemoteDisk final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromRemoteDiskTransaction;

    DiskPtr disk;
    std::string root_path_for_remote_metadata;

public:
    MetadataStorageFromRemoteDisk(DiskPtr disk_, const std::string & root_path_for_remote_metadata_)
        : disk(disk_)
        , root_path_for_remote_metadata(root_path_for_remote_metadata_)
    {
    }

    MetadataTransactionPtr createTransaction() const override;

    const std::string & getPath() const override;

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) override;

    std::string readFileToString(const std::string & path) const override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    BlobsPathToSize getBlobs(const std::string & path) const override;

    std::vector<std::string> getRemotePaths(const std::string & path) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    std::string getMetadataPath() const { return root_path_for_remote_metadata; }

    DiskPtr getDisk() const override { return disk; }

private:
    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;

    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> & lock) const;
};

class MetadataStorageFromRemoteDiskTransaction final : public MetadataStorageFromDiskTransaction
{
private:
    const MetadataStorageFromRemoteDisk & metadata_storage_for_remote;

public:
    explicit MetadataStorageFromRemoteDiskTransaction(const MetadataStorageFromRemoteDisk & metadata_storage_)
        : MetadataStorageFromDiskTransaction(metadata_storage_)
        , metadata_storage_for_remote(metadata_storage_)
    {}

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
};


}
