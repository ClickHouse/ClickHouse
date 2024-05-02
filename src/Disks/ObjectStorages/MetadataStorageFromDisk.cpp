#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Common/getRandomASCIIString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <ranges>
#include <filesystem>


namespace DB
{

MetadataStorageFromDisk::MetadataStorageFromDisk(DiskPtr disk_, String compatible_key_prefix_)
    : disk(disk_), compatible_key_prefix(compatible_key_prefix_)
{
}

const std::string & MetadataStorageFromDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromDisk::exists(const std::string & path) const
{
    return disk->exists(path);
}

bool MetadataStorageFromDisk::isFile(const std::string & path) const
{
    return disk->isFile(path);
}

bool MetadataStorageFromDisk::isDirectory(const std::string & path) const
{
    return disk->isDirectory(path);
}

Poco::Timestamp MetadataStorageFromDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

time_t MetadataStorageFromDisk::getLastChanged(const std::string & path) const
{
    return disk->getLastChanged(path);
}

uint64_t MetadataStorageFromDisk::getFileSize(const String & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getTotalSizeBytes();
}

std::vector<std::string> MetadataStorageFromDisk::listDirectory(const std::string & path) const
{
    std::vector<std::string> result_files;
    disk->listFiles(path, result_files);
    return result_files;
}

DirectoryIteratorPtr MetadataStorageFromDisk::iterateDirectory(const std::string & path) const
{
    return disk->iterateDirectory(path);
}


std::string MetadataStorageFromDisk::readFileToString(const std::string & path) const
{
    auto buf = disk->readFile(path);
    std::string result;
    readStringUntilEOF(result, *buf);
    return result;
}

std::string MetadataStorageFromDisk::readInlineDataToString(const std::string & path) const
{
    return readMetadata(path)->getInlineData();
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<SharedMutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::unique_lock<SharedMutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

std::unordered_map<String, String> MetadataStorageFromDisk::getSerializedMetadata(const std::vector<String> & file_paths) const
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

void MetadataStorageFromDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, *metadata_storage.disk, metadata_storage));
}

MetadataTransactionPtr MetadataStorageFromDisk::createTransaction()
{
    return std::make_shared<MetadataStorageFromDiskTransaction>(*this);
}

StoredObjects MetadataStorageFromDisk::getStorageObjects(const std::string & path) const
{
    auto metadata = readMetadata(path);
    const auto & keys_with_meta = metadata->getKeysWithMeta();

    StoredObjects objects;
    objects.reserve(keys_with_meta.size());
    for (const auto & [object_key, object_meta] : keys_with_meta)
    {
        objects.emplace_back(object_key.serialize(), path, object_meta.size_bytes);
    }

    return objects;
}

uint32_t MetadataStorageFromDisk::getHardlinkCount(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getRefCount();
}

const IMetadataStorage & MetadataStorageFromDiskTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void MetadataStorageFromDiskTransaction::commit()
{
    MetadataOperationsHolder::commitImpl(metadata_storage.metadata_mutex);
}

void MetadataStorageFromDiskTransaction::writeStringToFile(
     const std::string & path,
     const std::string & data)
{
    addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage.getDisk(), data));
}

void MetadataStorageFromDiskTransaction::writeInlineDataToFile(
     const std::string & path,
     const std::string & data)
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(metadata_storage.compatible_key_prefix, path);
    metadata->setInlineData(data);
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::chmod(const String & path, mode_t mode)
{
    addOperation(std::make_unique<ChmodOperation>(path, mode, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::unlinkFile(const std::string & path)
{
    addOperation(std::make_unique<UnlinkFileOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::removeRecursive(const std::string & path)
{
    addOperation(std::make_unique<RemoveRecursiveOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createDirectory(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createDirectoryRecursive(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::removeDirectory(const std::string & path)
{
    addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::setReadOnly(const std::string & path)
{
    auto metadata = metadata_storage.readMetadata(path);
    metadata->setReadOnly();
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromDiskTransaction::createEmptyMetadataFile(const std::string & path)
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(metadata_storage.compatible_key_prefix, path);
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromDiskTransaction::createMetadataFile(const std::string & path, ObjectStorageKey object_key, uint64_t size_in_bytes)
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(metadata_storage.compatible_key_prefix, path);
    metadata->addObject(std::move(object_key), size_in_bytes);

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage.getDisk(), data));
}

void MetadataStorageFromDiskTransaction::addBlobToMetadata(const std::string & path, ObjectStorageKey object_key, uint64_t size_in_bytes)
{
    addOperation(std::make_unique<AddBlobOperation>(path, std::move(object_key), size_in_bytes, *metadata_storage.disk, metadata_storage));
}

UnlinkMetadataFileOperationOutcomePtr MetadataStorageFromDiskTransaction::unlinkMetadata(const std::string & path)
{
    auto operation = std::make_unique<UnlinkMetadataFileOperation>(path, *metadata_storage.getDisk(), metadata_storage);
    auto result = operation->outcome;
    addOperation(std::move(operation));
    return result;
}

}
