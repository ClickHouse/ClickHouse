#include <Disks/ObjectStorages/MetadataStorageFromRemoteDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>
#include <ranges>
#include <filesystem>
#include <Disks/TemporaryFileOnDisk.h>
#include <Poco/TemporaryFile.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FS_METADATA_ERROR;
}


const std::string & MetadataStorageFromRemoteDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromRemoteDisk::exists(const std::string & path) const
{
    return disk->exists(path);
}

bool MetadataStorageFromRemoteDisk::isFile(const std::string & path) const
{
    return disk->isFile(path);
}


bool MetadataStorageFromRemoteDisk::isDirectory(const std::string & path) const
{
    return disk->isDirectory(path);
}

Poco::Timestamp MetadataStorageFromRemoteDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

uint64_t MetadataStorageFromRemoteDisk::getFileSize(const String & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getTotalSizeBytes();
}

std::vector<std::string> MetadataStorageFromRemoteDisk::listDirectory(const std::string & path) const
{
    std::vector<std::string> result_files;
    disk->listFiles(path, result_files);
    return result_files;
}

DirectoryIteratorPtr MetadataStorageFromRemoteDisk::iterateDirectory(const std::string & path)
{
    return disk->iterateDirectory(path);
}


std::string MetadataStorageFromRemoteDisk::readFileToString(const std::string & path) const
{
    auto buf = disk->readFile(path);
    std::string result;
    readStringUntilEOF(result, *buf);
    return result;
}

DiskObjectStorageMetadataPtr MetadataStorageFromRemoteDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), root_path_for_remote_metadata, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromRemoteDisk::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

std::unordered_map<String, String> MetadataStorageFromRemoteDisk::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    std::shared_lock lock(metadata_mutex);
    std::unordered_map<String, String> metadatas;

    for (const auto & path : file_paths)
    {
        auto metadata = readMetadataUnlocked(path, lock);
        metadata->resetRefCount();
        WriteBufferFromOwnString buf;
        metadata->serialize(buf, false);
        metadatas[path] = buf.str();
    }

    return metadatas;
}

void MetadataStorageFromRemoteDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    auto metadata = metadata_storage_for_remote.readMetadata(path_from);

    metadata->incrementRefCount();

    writeStringToFile(path_from, metadata->serializeToString());

    addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

MetadataTransactionPtr MetadataStorageFromRemoteDisk::createTransaction() const
{
    return std::make_shared<MetadataStorageFromRemoteDiskTransaction>(*this);
}

std::vector<std::string> MetadataStorageFromRemoteDisk::getRemotePaths(const std::string & path) const
{
    auto metadata = readMetadata(path);

    std::vector<std::string> remote_paths;
    auto blobs = metadata->getBlobs();
    auto root_path = metadata->getBlobsCommonPrefix();
    remote_paths.reserve(blobs.size());
    for (const auto & [remote_path, _] : blobs)
        remote_paths.push_back(fs::path(root_path) / remote_path);

    return remote_paths;
}

uint32_t MetadataStorageFromRemoteDisk::getHardlinkCount(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getRefCount();
}

BlobsPathToSize MetadataStorageFromRemoteDisk::getBlobs(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getBlobs();
}

void MetadataStorageFromRemoteDiskTransaction::writeStringToFile( /// NOLINT
     const std::string & path,
     const std::string & data)
{
    addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::unlinkFile(const std::string & path)
{
    addOperation(std::make_unique<UnlinkFileOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::removeRecursive(const std::string & path)
{
    addOperation(std::make_unique<RemoveRecursiveOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::createDirectory(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::createDicrectoryRecursive(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::removeDirectory(const std::string & path)
{
    addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::setReadOnly(const std::string & path)
{
    auto metadata = metadata_storage_for_remote.readMetadata(path);
    metadata->setReadOnly();

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::createEmptyMetadataFile(const std::string & path)
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(
        metadata_storage_for_remote.getDisk()->getPath(), metadata_storage_for_remote.getMetadataPath(), path);

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    DiskObjectStorageMetadataPtr metadata = std::make_unique<DiskObjectStorageMetadata>(
        metadata_storage_for_remote.getDisk()->getPath(), metadata_storage_for_remote.getMetadataPath(), path);

    metadata->addObject(blob_name, size_in_bytes);

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    DiskObjectStorageMetadataPtr metadata;
    if (metadata_storage_for_remote.exists(path))
    {
        metadata = metadata_storage_for_remote.readMetadata(path);
        metadata->addObject(blob_name, size_in_bytes);

        auto data = metadata->serializeToString();
        if (!data.empty())
            addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
    }
    else
    {
        createMetadataFile(path, blob_name, size_in_bytes);
    }
}

void MetadataStorageFromRemoteDiskTransaction::unlinkMetadata(const std::string & path)
{
    auto metadata = metadata_storage_for_remote.readMetadata(path);
    uint32_t ref_count = metadata->getRefCount();
    if (ref_count != 0)
    {
        metadata->decrementRefCount();
        writeStringToFile(path, metadata->serializeToString());
    }
    unlinkFile(path);
}

}
