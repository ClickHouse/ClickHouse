#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorageTransaction.h"
#include "VFSTransactionLog.h"

namespace DB
{
struct DiskObjectStorageVFSTransaction final : public DiskObjectStorageTransaction
{
    DiskObjectStorageVFSTransaction( //NOLINT
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        zkutil::ZooKeeperPtr zookeeper_);

    void replaceFile(const String & from_path, const String & to_path) override;

    // createFile creates an empty file. If the file is written using writeFile, we'd
    // account hardlinks, if it's not, no need to track it.

    void copyFile(
        const String & from_file_path,
        const String & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {},
        bool autocommit = true) override;

    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override;

    void removeFileIfExists(const String & path) override;
    // removeDirectory handles only empty directories, so no hardlink updates
    void removeSharedFile(const String & path, bool) override;
    void removeSharedRecursive(const String & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only) override;
    void removeSharedFileIfExists(const String & path, bool keep_shared_data) override;
    void
    removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override;

    void createHardLink(const String & src_path, const String & dst_path) override;
    void createLink(const String & path) override;

    auto shared_from_this()
    {
        return std::static_pointer_cast<DiskObjectStorageVFSTransaction>(DiskObjectStorageTransaction::shared_from_this());
    }

private:
    zkutil::ZooKeeperPtr zookeeper;
    Poco::Logger const * const log;
    void addStoredObjectsOp(VFSTransactionLogItem::Type type, const StoredObjects & objects);
};
}
