#pragma once
#include <Disks/IDiskTransaction.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>

namespace DB
{


struct IDiskObjectStorageOperation
{
    DiskObjectStorage & disk;
public:
    explicit IDiskObjectStorageOperation(DiskObjectStorage & disk_)
        : disk(disk_)
    {}
    virtual void execute() = 0;
    virtual void undo() = 0;
    virtual ~IDiskObjectStorageOperation() = default;
};

using DiskObjectStorageOperation = std::unique_ptr<IDiskObjectStorageOperation>;
using DiskObjectStorageOperations = std::vector<DiskObjectStorageOperation>;

struct DiskObjectStorageTransaction : public IDiskTransaction
{
private:
    DiskObjectStorage & disk;
    MetadataTransactionPtr metadata_transaction;
public:
    explicit DiskObjectStorageTransaction(DiskObjectStorage & disk_)
        : disk(disk_)
        , metadata_transaction(disk.getMetadataStorage()->createTransaction())
    {}

    void commit() override;

    void createDirectory(const std::string & path) override;

    void createDirectories(const std::string & path) override;

    /// Remove all files from the directory. Directories are not removed.
    void clearDirectory(const std::string & path) override;

    /// Move directory from `from_path` to `to_path`.
    void moveDirectory(const std::string & from_path, const std::string & to_path) override;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    void replaceFile(const std::string & from_path, const std::string & to_path) override;

    /// Recursively copy data containing at `from_path` to `to_path` located at `to_disk`.
    void copy(const std::string & from_path, const std::shared_ptr<IDisk> & to_disk, const std::string & to_path) override;

    /// Recursively copy files from from_dir to to_dir. Create to_dir if not exists.
    void copyDirectoryContent(const std::string & from_dir, const std::shared_ptr<IDisk> & to_disk, const std::string & to_dir) override;

    /// Copy file `from_file_path` to `to_file_path` located at `to_disk`.
    void copyFile(const std::string & from_file_path, IDisk & to_disk, const std::string & to_file_path) override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const std::string & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {}) override;

    void removeFile(const std::string & path) override;
    void removeFileIfExists(const std::string & path) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path) override;
    void removeSharedFile(const std::string & path, bool /* keep_shared_data */) override;
    void removeSharedRecursive(const std::string & path, bool /* keep_all_shared_data */, const NameSet & /* file_names_remove_metadata_only */) override;
    void removeSharedFileIfExists(const std::string & path, bool /* keep_shared_data */) override;
    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;
    void setReadOnly(const std::string & path) override;
    void createHardLink(const std::string & src_path, const std::string & dst_path) override;
    void truncateFile(const std::string & path, size_t size) override;

};

}
