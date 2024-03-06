#pragma once
#include "DiskObjectStorageTransaction.h"
#include "VFSLogItem.h"

namespace DB
{
class DiskObjectStorageVFS;

struct VFSTransaction : DiskObjectStorageTransaction, private VFSLogItem
{
    VFSTransaction(DiskObjectStorageVFS & disk_); // NOLINT
    void commit() override;

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

    auto shared_from_this() { return std::static_pointer_cast<VFSTransaction>(DiskObjectStorageTransaction::shared_from_this()); }

protected:
    DiskObjectStorageVFS & disk;
    VFSLogItem & item; // Points to VFSTransactionGroup if there's any, points to *this otherwise

    void addStoredObjectsOp(StoredObjects && link, StoredObjects && unlink);
};

struct MultipleDisksVFSTransaction final : VFSTransaction
{
    IObjectStorage & destination_object_storage;
    MultipleDisksVFSTransaction(DiskObjectStorageVFS & disk_, IObjectStorage & destination_object_storage_);

    auto shared_from_this() { return std::static_pointer_cast<MultipleDisksVFSTransaction>(VFSTransaction::shared_from_this()); }

    void copyFile(
        const String & from_file_path,
        const String & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) override;
};
}
