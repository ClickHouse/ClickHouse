#include "MetadataStorageFromLocalDisk.h"
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/IDisk.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MetadataStorageFromLocalDisk::MetadataStorageFromLocalDisk(DiskPtr disk_)
    : disk(disk_)
{
}

MetadataTransactionPtr MetadataStorageFromLocalDisk::createTransaction() const
{
    return std::make_shared<MetadataStorageFromLocalDiskTransaction>(*this);
}

const std::string & MetadataStorageFromLocalDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromLocalDisk::exists(const std::string & path) const
{
    return disk->exists(path);
}

bool MetadataStorageFromLocalDisk::isFile(const std::string & path) const
{
    return disk->isFile(path);
}

bool MetadataStorageFromLocalDisk::isDirectory(const std::string & path) const
{
    return disk->isDirectory(path);
}

Poco::Timestamp MetadataStorageFromLocalDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

time_t MetadataStorageFromLocalDisk::getLastChanged(const std::string & path) const
{
    return disk->getLastChanged(path);
}

uint64_t MetadataStorageFromLocalDisk::getFileSize(const String & path) const
{
    return disk->getFileSize(path);
}

std::vector<std::string> MetadataStorageFromLocalDisk::listDirectory(const std::string & path) const
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

DirectoryIteratorPtr MetadataStorageFromLocalDisk::iterateDirectory(const std::string & path) const
{
    return disk->iterateDirectory(path);
}

std::string MetadataStorageFromLocalDisk::readFileToString(const std::string &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "readFileToString is not implemented for MetadataStorageFromLocalDisk");
}

std::unordered_map<String, String> MetadataStorageFromLocalDisk::getSerializedMetadata(const std::vector<String> &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getSerializedMetadata is not implemented for MetadataStorageFromLocalDisk");
}

BlobsPathToSize MetadataStorageFromLocalDisk::getBlobs(const std::string & path) const
{
    return {BlobPathWithSize(path, getFileSize(path))};
}

std::vector<std::string> MetadataStorageFromLocalDisk::getRemotePaths(const std::string & path) const
{
    return {fs::path(getPath()) / path};
}

uint32_t MetadataStorageFromLocalDisk::getHardlinkCount(const std::string & path) const
{
    /// FIXME: -1?
    return disk->getRefCount(path);
}

void MetadataStorageFromLocalDiskTransaction::writeStringToFile( /// NOLINT
     const std::string & path,
     const std::string & data)
{
    addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_local.getDisk(), data));
}

void MetadataStorageFromLocalDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::unlinkFile(const std::string & path)
{
    addOperation(std::make_unique<UnlinkFileOperation>(path, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::removeRecursive(const std::string & path)
{
    addOperation(std::make_unique<RemoveRecursiveOperation>(path, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::createDirectory(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::createDicrectoryRecursive(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::removeDirectory(const std::string & path)
{
    addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::setReadOnly(const std::string & path)
{
    addOperation(std::make_unique<SetReadOnlyOperation>(path, *metadata_storage_for_local.getDisk()));
}

void MetadataStorageFromLocalDiskTransaction::createHardLink(const std::string & /* path_from */, const std::string & /* path_from */)
{
}

void MetadataStorageFromLocalDiskTransaction::createEmptyMetadataFile(const std::string & /* path */)
{
}

void MetadataStorageFromLocalDiskTransaction::createMetadataFile(
    const std::string & /* path */, const std::string & /* blob_name */, uint64_t /* size_in_bytes */)
{
}

void MetadataStorageFromLocalDiskTransaction::addBlobToMetadata(
    const std::string & /* path */, const std::string & /* blob_name */, uint64_t /* size_in_bytes */)
{
}

void MetadataStorageFromLocalDiskTransaction::unlinkMetadata(const std::string & path)
{
    addOperation(std::make_unique<UnlinkFileOperation>(path, *metadata_storage_for_local.getDisk()));
}

}
