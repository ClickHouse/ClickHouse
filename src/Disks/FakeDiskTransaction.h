#pragma once

#include <Disks/IDiskTransaction.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Fake disk transaction implementation.
/// Just execute all operations immediately, commit is noop operation.
/// No support for atomicity and rollback.
struct FakeDiskTransaction final : public IDiskTransaction
{
public:
    explicit FakeDiskTransaction(IDisk & disk_)
        : disk(disk_)
    {}

    void commit() override {}
    void undo() override {}

    void createDirectory(const std::string & path) override
    {
        disk.createDirectory(path);
    }

    void createDirectories(const std::string & path) override
    {
        disk.createDirectories(path);
    }

    void createFile(const std::string & path) override
    {
        disk.createFile(path);
    }

    void clearDirectory(const std::string & path) override
    {
        disk.createDirectory(path);
    }

    void moveDirectory(const std::string & from_path, const std::string & to_path) override
    {
        disk.moveDirectory(from_path, to_path);
    }

    void moveFile(const String & from_path, const String & to_path) override
    {
        disk.moveFile(from_path, to_path);
    }

    void replaceFile(const std::string & from_path, const std::string & to_path) override
    {
        disk.replaceFile(from_path, to_path);
    }

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings) override
    {
        disk.copyFile(from_file_path, disk, to_file_path, read_settings, write_settings);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override
    {
        return disk.writeFile(path, buf_size, mode, settings);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override
    {
        return disk.writeFile(path, buf_size, mode, settings);
    }

    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override
    {
        disk.writeFileUsingBlobWritingFunction(path, mode, std::move(write_blob_function));
    }

    void removeFile(const std::string & path) override
    {
        disk.removeFile(path);
    }

    void removeFileIfExists(const std::string & path) override
    {
        disk.removeFileIfExists(path);
    }

    void removeDirectory(const std::string & path) override
    {
        disk.removeDirectory(path);
    }

    void removeRecursive(const std::string & path) override
    {
        disk.removeRecursive(path);
    }

    void removeSharedFile(const std::string & path, bool keep_shared_data) override
    {
        disk.removeSharedFile(path, keep_shared_data);
    }

    void removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only) override
    {
        disk.removeSharedRecursive(path, keep_all_shared_data, file_names_remove_metadata_only);
    }

    void removeSharedFileIfExists(const std::string & path, bool keep_shared_data) override
    {
        disk.removeSharedFileIfExists(path, keep_shared_data);
    }

    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override
    {
        disk.removeSharedFiles(files, keep_all_batch_data, file_names_remove_metadata_only);
    }

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override
    {
        disk.setLastModified(path, timestamp);
    }

    void chmod(const String & path, mode_t mode) override
    {
        disk.chmod(path, mode);
    }

    void setReadOnly(const std::string & path) override
    {
        disk.setReadOnly(path);
    }

    void createHardLink(const std::string & src_path, const std::string & dst_path) override
    {
        disk.createHardLink(src_path, dst_path);
    }

    void truncateFile(const std::string & /* src_path */, size_t /* target_size */) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation `truncateFile` is not implemented");
    }

private:
    IDisk & disk;
};

}
