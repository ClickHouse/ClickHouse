#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <ranges>
#include <filesystem>
#include <Disks/TemporaryFileOnDisk.h>
#include <Poco/TemporaryFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/getRandomASCIIString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FS_METADATA_ERROR;
}


std::string toString(MetadataFromDiskTransactionState state)
{
    switch (state)
    {
        case MetadataFromDiskTransactionState::PREPARING:
            return "PREPARING";
        case MetadataFromDiskTransactionState::FAILED:
            return "FAILED";
        case MetadataFromDiskTransactionState::COMMITTED:
            return "COMMITTED";
        case MetadataFromDiskTransactionState::PARTIALLY_ROLLED_BACK:
            return "PARTIALLY_ROLLED_BACK";
    }
    __builtin_unreachable();
}

void MetadataStorageFromDiskTransaction::writeStringToFile( /// NOLINT
     const std::string & path,
     const std::string & data)
{
    addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage.disk, data));
}


void MetadataStorageFromDiskTransaction::addOperation(MetadataOperationPtr && operation)
{
    if (state != MetadataFromDiskTransactionState::PREPARING)
        throw Exception(ErrorCodes::FS_METADATA_ERROR, "Cannot add operations to transaction in {} state, it should be in {} state",
                        toString(state), toString(MetadataFromDiskTransactionState::PREPARING));

    operations.emplace_back(std::move(operation));
}

void MetadataStorageFromDiskTransaction::commit()
{
    if (state != MetadataFromDiskTransactionState::PREPARING)
        throw Exception(ErrorCodes::FS_METADATA_ERROR, "Cannot commit transaction in {} state, it should be in {} state",
                        toString(state), toString(MetadataFromDiskTransactionState::PREPARING));

    {
        std::lock_guard lock(metadata_storage.metadata_mutex);
        for (size_t i = 0; i < operations.size(); ++i)
        {
            try
            {
                operations[i]->execute();
            }
            catch (Exception & ex)
            {
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

void MetadataStorageFromDiskTransaction::createEmptyMetadataFile(const std::string & path)
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(metadata_storage.disk->getPath(), metadata_storage.root_path_for_remote_metadata, path);
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::unlinkFile(const std::string & path)
{
    addOperation(std::make_unique<UnlinkFileOperation>(path, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::removeRecursive(const std::string & path)
{
    addOperation(std::make_unique<RemoveRecursiveOperation>(path, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::createDirectory(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::createDicrectoryRecursive(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::removeDirectory(const std::string & path)
{
    addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    auto metadata = metadata_storage.readMetadata(path_from);

    metadata->incrementRefCount();

    writeStringToFile(path_from, metadata->serializeToString());

    addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::setReadOnly(const std::string & path)
{
    auto metadata = metadata_storage.readMetadata(path);
    metadata->setReadOnly();
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromDiskTransaction::createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    DiskObjectStorageMetadataPtr metadata = std::make_unique<DiskObjectStorageMetadata>(metadata_storage.disk->getPath(), metadata_storage.root_path_for_remote_metadata, path);
    metadata->addObject(blob_name, size_in_bytes);
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromDiskTransaction::addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    DiskObjectStorageMetadataPtr metadata;
    if (metadata_storage.exists(path))
    {
        metadata = metadata_storage.readMetadata(path);
    }
    else
    {
        metadata = std::make_unique<DiskObjectStorageMetadata>(metadata_storage.disk->getPath(), metadata_storage.root_path_for_remote_metadata, path);
    }

    metadata->addObject(blob_name, size_in_bytes);
    writeStringToFile(path, metadata->serializeToString());
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), root_path_for_remote_metadata, path);
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

MetadataTransactionPtr MetadataStorageFromDisk::createTransaction() const
{
    return std::make_shared<MetadataStorageFromDiskTransaction>(*this);
}

PathsWithSize MetadataStorageFromDisk::getObjectStoragePaths(const std::string & path) const
{
    auto metadata = readMetadata(path);

    auto object_storage_relative_paths = metadata->getBlobsRelativePaths(); /// Relative paths.
    fs::path root_path = metadata->getBlobsCommonPrefix();

    PathsWithSize object_storage_paths;
    object_storage_paths.reserve(object_storage_relative_paths.size());

    /// Relative paths -> absolute.
    for (auto & [object_relative_path, size] : object_storage_relative_paths)
        object_storage_paths.emplace_back(root_path / object_relative_path, size);

    return object_storage_paths;
}

uint32_t MetadataStorageFromDisk::getHardlinkCount(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getRefCount();
}

void MetadataStorageFromDiskTransaction::unlinkMetadata(const std::string & path)
{
    auto metadata = metadata_storage.readMetadata(path);
    uint32_t ref_count = metadata->getRefCount();
    if (ref_count != 0)
    {
        metadata->decrementRefCount();
        writeStringToFile(path, metadata->serializeToString());
    }
    unlinkFile(path);
}

}
