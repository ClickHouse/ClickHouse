#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Common/getRandomASCIIString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/TemporaryFile.h>
#include <ranges>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int FS_METADATA_ERROR;
}

MetadataStorageFromDisk::MetadataStorageFromDisk(DiskPtr disk_, const std::string & object_storage_root_path_)
    : disk(disk_)
    , object_storage_root_path(object_storage_root_path_)
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

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), object_storage_root_path, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::unique_lock<std::shared_mutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), object_storage_root_path, path);
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

MetadataTransactionPtr MetadataStorageFromDisk::createTransaction() const
{
    return std::make_shared<MetadataStorageFromDiskTransaction>(*this);
}

StoredObjects MetadataStorageFromDisk::getStorageObjects(const std::string & path) const
{
    auto metadata = readMetadata(path);

    auto object_storage_relative_paths = metadata->getBlobsRelativePaths(); /// Relative paths.

    StoredObjects object_storage_paths;
    object_storage_paths.reserve(object_storage_relative_paths.size());

    /// Relative paths -> absolute.
    for (auto & [object_relative_path, size] : object_storage_relative_paths)
    {
        auto object_path = fs::path(metadata->getBlobsCommonPrefix()) / object_relative_path;
        StoredObject object{ object_path, size, [](const String & path_){ return path_; }};
        object_storage_paths.push_back(object);
    }

    return object_storage_paths;
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

void MetadataStorageFromDiskTransaction::addOperation(MetadataOperationPtr && operation)
{
    if (state != MetadataFromDiskTransactionState::PREPARING)
        throw Exception(
            ErrorCodes::FS_METADATA_ERROR,
            "Cannot add operations to transaction in {} state, it should be in {} state",
            toString(state), toString(MetadataFromDiskTransactionState::PREPARING));

    operations.emplace_back(std::move(operation));
}

void MetadataStorageFromDiskTransaction::commit()
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
                operations[i]->execute(lock);
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

void MetadataStorageFromDiskTransaction::rollback(size_t until_pos)
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

void MetadataStorageFromDiskTransaction::writeStringToFile(
     const std::string & path,
     const std::string & data)
{
    addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage.getDisk(), data));
}

void MetadataStorageFromDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage.getDisk()));
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
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(
        metadata_storage.getDisk()->getPath(), metadata_storage.getObjectStorageRootPath(), path);
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromDiskTransaction::createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    DiskObjectStorageMetadataPtr metadata = std::make_unique<DiskObjectStorageMetadata>(
        metadata_storage.getDisk()->getPath(), metadata_storage.getObjectStorageRootPath(), path);

    metadata->addObject(blob_name, size_in_bytes);

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage.getDisk(), data));
}

void MetadataStorageFromDiskTransaction::addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    addOperation(std::make_unique<AddBlobOperation>(path, blob_name, metadata_storage.object_storage_root_path, size_in_bytes, *metadata_storage.disk, metadata_storage));
}

void MetadataStorageFromDiskTransaction::unlinkMetadata(const std::string & path)
{
    addOperation(std::make_unique<UnlinkMetadataFileOperation>(path, *metadata_storage.disk, metadata_storage));
}

}
