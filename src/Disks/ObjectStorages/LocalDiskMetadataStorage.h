#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/DiskLocal.h>

namespace DB
{

enum class LocalMetadataTransactionState
{
    PREPARING,
    COMMITED,
    ROLLED_BACK
};

struct LocalDiskMetadataTransaction : public IMetadataTransaction
{
private:
    std::vector<MetadataOperationPtr> operations;
    LocalMetadataTransactionState state{LocalMetadataTransactionState::PREPARING};
public:
    void addOperation(MetadataOperationPtr && operation) override
    {
        operations.emplace_back(std::move(operation));
    }

    void commit() override;
    void rollback() override;
};

class LocalDiskMetadataStorage : public IMetadataStorage
{
private:
    DiskPtr local_disk;
public:
    explicit LocalDiskMetadataStorage(DiskPtr local_disk_)
        : local_disk(local_disk_)
    {
    }

    MetadataTransactionPtr createTransaction() const
    {
        return std::make_shared<LocalDiskMetadataTransaction>(local_disk);
    }

    bool exists(const std::string & path) const override
    {
        return local_disk->exists(path);
    }

    bool isFile(const std::string & path) const override
    {
        return local_disk->isFile(path);
    }

    bool isDirectory(const std::string & path) const override
    {
        return local_disk->isDirectory(path);
    }

    Poco::Timestamp getLastModified(const std::string & path) const override
    {
        return local_disk->getLastModified(path);
    }

    std::vector<std::string> listDirectory(const std::string & path) const override
    {
        std::vector<std::string> result_files;
        local_disk->listFiles(path, result_files);
        return result_files;
    }

    std::unique_ptr<ReadBufferFromFileBase> readFile(  /// NOLINT
         const std::string & path,
         const ReadSettings & settings = ReadSettings{},
         std::optional<size_t> read_hint = {},
         std::optional<size_t> file_size = {}) const override
    {
        return local_disk->readFile(path, settings, read_hint, file_size);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
         const std::string & path,
         MetadataTransactionPtr transaction,
         size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
         const WriteSettings & settings = {}) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp, MetadataTransactionPtr transaction) override;

    void unlinkFile(const std::string & path, MetadataTransactionPtr transaction) override;

    void createDirectory(const std::string & path, MetadataTransactionPtr transaction) override;

    void createDicrectoryRecursive(const std::string & path, MetadataTransactionPtr transaction) override;

    void removeDirectory(const std::string & path, MetadataTransactionPtr transaction) override;

    void createHardlink(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void moveFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void replaceFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

};


}
