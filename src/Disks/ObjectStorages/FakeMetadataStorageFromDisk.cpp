#include "FakeMetadataStorageFromDisk.h"
#include <Disks/IDisk.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int FS_METADATA_ERROR;
}

FakeMetadataStorageFromDisk::FakeMetadataStorageFromDisk(
    DiskPtr disk_,
    ObjectStoragePtr object_storage_,
    const std::string & object_storage_root_path_)
    : disk(disk_)
    , object_storage(object_storage_)
    , object_storage_root_path(object_storage_root_path_)
{
}

MetadataTransactionPtr FakeMetadataStorageFromDisk::createTransaction() const
{
    return std::make_shared<FakeMetadataStorageFromDiskTransaction>(*this, disk);
}

const std::string & FakeMetadataStorageFromDisk::getPath() const
{
    return disk->getPath();
}

bool FakeMetadataStorageFromDisk::exists(const std::string & path) const
{
    return disk->exists(path);
}

bool FakeMetadataStorageFromDisk::isFile(const std::string & path) const
{
    return disk->isFile(path);
}

bool FakeMetadataStorageFromDisk::isDirectory(const std::string & path) const
{
    return disk->isDirectory(path);
}

Poco::Timestamp FakeMetadataStorageFromDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

time_t FakeMetadataStorageFromDisk::getLastChanged(const std::string & path) const
{
    return disk->getLastChanged(path);
}

uint64_t FakeMetadataStorageFromDisk::getFileSize(const String & path) const
{
    return disk->getFileSize(path);
}

std::vector<std::string> FakeMetadataStorageFromDisk::listDirectory(const std::string & path) const
{
    std::vector<std::string> result;
    auto it = disk->iterateDirectory(path);
    while (it->isValid())
    {
        result.push_back(it->path());
        it->next();
    }
    return result;
}

DirectoryIteratorPtr FakeMetadataStorageFromDisk::iterateDirectory(const std::string & path) const
{
    return disk->iterateDirectory(path);
}

std::string FakeMetadataStorageFromDisk::readFileToString(const std::string &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "readFileToString is not implemented for FakeMetadataStorageFromDisk");
}

std::unordered_map<String, String> FakeMetadataStorageFromDisk::getSerializedMetadata(const std::vector<String> &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getSerializedMetadata is not implemented for FakeMetadataStorageFromDisk");
}

StoredObjects FakeMetadataStorageFromDisk::getStorageObjects(const std::string & path) const
{
    std::string blob_name = object_storage->generateBlobNameForPath(path);

    std::string object_path = fs::path(object_storage_root_path) / blob_name;
    size_t object_size = getFileSize(object_path);

    auto object = StoredObject::create(*object_storage, object_path, object_size);
    return {std::move(object)};
}

uint32_t FakeMetadataStorageFromDisk::getHardlinkCount(const std::string & path) const
{
    size_t ref_count = disk->getRefCount(path);
    assert(ref_count > 0);
    return ref_count - 1;
}

const IMetadataStorage & FakeMetadataStorageFromDiskTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void FakeMetadataStorageFromDiskTransaction::addOperation(MetadataOperationPtr && operation)
{
    if (state != MetadataFromDiskTransactionState::PREPARING)
        throw Exception(
            ErrorCodes::FS_METADATA_ERROR,
            "Cannot add operations to transaction in {} state, it should be in {} state",
            toString(state), toString(MetadataFromDiskTransactionState::PREPARING));

    operations.emplace_back(std::move(operation));
}

void FakeMetadataStorageFromDiskTransaction::commit()
{
    if (state != MetadataFromDiskTransactionState::PREPARING)
        throw Exception(
            ErrorCodes::FS_METADATA_ERROR,
            "Cannot commit transaction in {} state, it should be in {} state",
            toString(state), toString(MetadataFromDiskTransactionState::PREPARING));

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);
        for (size_t i = 0; i < operations.size(); ++i)
        {
            try
            {
                operations[i]->execute();
            }
            catch (Exception & ex)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                ex.addMessage(fmt::format("While committing metadata operation #{}", i));
                state = MetadataFromDiskTransactionState::FAILED;
                rollback(i);
                throw;
            }
        }
    }

    /// Do it in "best effort" mode
    for (size_t i = 0; i < operations.size(); ++i)
    {
        try
        {
            operations[i]->finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Failed to finalize operation #{}", i));
        }
    }

    state = MetadataFromDiskTransactionState::COMMITTED;
}

void FakeMetadataStorageFromDiskTransaction::rollback(size_t until_pos)
{
    /// Otherwise everything is alright
    if (state == MetadataFromDiskTransactionState::FAILED)
    {
        for (int64_t i = until_pos; i >= 0; --i)
        {
            try
            {
                operations[i]->undo();
            }
            catch (Exception & ex)
            {
                state = MetadataFromDiskTransactionState::PARTIALLY_ROLLED_BACK;
                ex.addMessage(fmt::format("While rolling back operation #{}", i));
                throw;
            }
        }
    }
    else
    {
        /// Nothing to do, transaction committed or not even started to commit
    }
}

void FakeMetadataStorageFromDiskTransaction::writeStringToFile(const std::string & path, const std::string & data)
{
    auto wb = disk->writeFile(path);
    wb->write(data.data(), data.size());
    wb->finalize();
}

void FakeMetadataStorageFromDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    disk->setLastModified(path, timestamp);
}

void FakeMetadataStorageFromDiskTransaction::unlinkFile(const std::string & path)
{
    disk->removeFile(path);
}

void FakeMetadataStorageFromDiskTransaction::removeRecursive(const std::string & path)
{
    disk->removeRecursive(path);
}

void FakeMetadataStorageFromDiskTransaction::createDirectory(const std::string & path)
{
    disk->createDirectory(path);
}

void FakeMetadataStorageFromDiskTransaction::createDirectoryRecursive(const std::string & path)
{
    disk->createDirectories(path);
}

void FakeMetadataStorageFromDiskTransaction::removeDirectory(const std::string & path)
{
    disk->removeDirectory(path);
}

void FakeMetadataStorageFromDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    disk->moveFile(path_from, path_to);
}

void FakeMetadataStorageFromDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    disk->moveDirectory(path_from, path_to);
}

void FakeMetadataStorageFromDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    disk->replaceFile(path_from, path_to);
}

void FakeMetadataStorageFromDiskTransaction::setReadOnly(const std::string & path)
{
    disk->setReadOnly(path);
}

void FakeMetadataStorageFromDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    disk->createHardLink(path_from, path_to);
}

void FakeMetadataStorageFromDiskTransaction::createEmptyMetadataFile(const std::string & /* path */)
{
    /// Noop.
}

void FakeMetadataStorageFromDiskTransaction::createMetadataFile(
    const std::string & /* path */, const std::string & /* blob_name */, uint64_t /* size_in_bytes */)
{
    /// Noop.
}

void FakeMetadataStorageFromDiskTransaction::addBlobToMetadata(
    const std::string & /* path */, const std::string & /* blob_name */, uint64_t /* size_in_bytes */)
{
    /// Noop, local metadata files is only one file, it is the metadata file itself.
}

void FakeMetadataStorageFromDiskTransaction::unlinkMetadata(const std::string & path)
{
    disk->removeFile(path);
}

}
