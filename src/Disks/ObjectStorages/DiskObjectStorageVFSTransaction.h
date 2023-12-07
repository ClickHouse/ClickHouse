#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorageTransaction.h"
#include "VFSTraits.h"
#include "VFSTransactionLog.h"

namespace DB
{


struct DiskObjectStorageVFSTransaction final : public DiskObjectStorageTransaction
{
    DiskObjectStorageVFSTransaction(
        IObjectStorage & object_storage_, IMetadataStorage & metadata_storage_, zkutil::ZooKeeperPtr zookeeper_, const VFSTraits & traits_);

    void replaceFile(const String & from_path, const String & to_path) override;

    // createFile creates an empty file. If the file is written using writeFile, we'd
    // account hardlinks, if it's not, no need to track it.

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

private:
    zkutil::ZooKeeperPtr zookeeper;
    Poco::Logger const * const log;
    VFSTraits traits;

    void addStoredObjectsOp(VFSTransactionLogItem::Type type, StoredObjects && objects);
};
}
