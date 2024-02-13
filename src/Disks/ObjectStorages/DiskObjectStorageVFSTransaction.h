#pragma once
#include "DiskObjectStorageTransaction.h"

namespace DB
{
class DiskObjectStorageVFS;

struct DiskObjectStorageVFSTransaction : public DiskObjectStorageTransaction
{
    DiskObjectStorageVFSTransaction(DiskObjectStorageVFS & disk_); // NOLINT
    void replaceFile(const String & from_path, const String & to_path) override;

    void copyFile(
        const String & from_file_path,
        const String & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) override;

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings, bool autocommit) override;

    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override;

    void removeFileIfExists(const String & path) override;
    // removeDirectory handles only empty directories, so no hardlink updates
    void removeSharedFile(const String & path, bool) override;
    void removeSharedRecursive(const String & path, bool, const NameSet &) override;
    void removeSharedFileIfExists(const String & path, bool) override;
    void removeSharedFiles(const RemoveBatchRequest & files, bool, const NameSet &) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    auto shared_from_this()
    {
        return std::static_pointer_cast<DiskObjectStorageVFSTransaction>(DiskObjectStorageTransaction::shared_from_this());
    }

protected:
    DiskObjectStorageVFS & disk;

    void addStoredObjectsOp(StoredObjects && link, StoredObjects && unlink);
};

struct MultipleDisksObjectStorageVFSTransaction final : DiskObjectStorageVFSTransaction
{
    IObjectStorage & destination_object_storage;
    MultipleDisksObjectStorageVFSTransaction(DiskObjectStorageVFS & disk_, IObjectStorage & destination_object_storage_);

    auto shared_from_this()
    {
        return std::static_pointer_cast<MultipleDisksObjectStorageVFSTransaction>(DiskObjectStorageVFSTransaction::shared_from_this());
    }

    void copyFile(
        const String & from_file_path,
        const String & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) override;
};
}
