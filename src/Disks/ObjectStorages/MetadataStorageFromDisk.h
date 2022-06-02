#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <Disks/IDisk.h>

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

struct MetadataStorageFromDiskTransaction : public IMetadataTransaction
{
private:
    std::optional<size_t> failed_operation_index;

    std::vector<MetadataOperationPtr> operations;
    MetadataFromDiskTransactionState state{MetadataFromDiskTransactionState::PREPARING};
public:
    void addOperation(MetadataOperationPtr && operation) override;
    void commit() override;
    void rollback() override;

    ~MetadataStorageFromDiskTransaction() override;
};

class MetadataStorageFromDisk : public IMetadataStorage
{
private:
    DiskPtr disk;
public:
    explicit MetadataStorageFromDisk(DiskPtr disk_)
        : disk(disk_)
    {
    }

    MetadataTransactionPtr createTransaction() const override
    {
        return std::make_shared<MetadataStorageFromDiskTransaction>();
    }

    const std::string & getPath() const override;

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

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

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp, MetadataTransactionPtr transaction) override;

    void unlinkFile(const std::string & path, MetadataTransactionPtr transaction) override;

    void createDirectory(const std::string & path, MetadataTransactionPtr transaction) override;

    void createDicrectoryRecursive(const std::string & path, MetadataTransactionPtr transaction) override;

    void removeDirectory(const std::string & path, MetadataTransactionPtr transaction) override;

    void removeRecursive(const std::string & path, MetadataTransactionPtr transaction) override;

    void createHardLink(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void moveFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void moveDirectory(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

    void replaceFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) override;

};


}
