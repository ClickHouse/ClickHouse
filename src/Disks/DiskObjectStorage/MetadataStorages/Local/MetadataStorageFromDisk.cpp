#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/logger_useful.h>

#include <limits>
#include <ranges>
#include <memory>
#include <shared_mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MetadataStorageFromDisk::MetadataStorageFromDisk(DiskPtr disk_, std::string compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_)
    : disk(disk_)
    , compatible_key_prefix(std::move(compatible_key_prefix_))
    , key_generator(std::move(key_generator_))
{
}

const std::string & MetadataStorageFromDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromDisk::existsFile(const std::string & path) const
{
    return disk->existsFile(path);
}

bool MetadataStorageFromDisk::existsDirectory(const std::string & path) const
{
    return disk->existsDirectory(path);
}

bool MetadataStorageFromDisk::existsFileOrDirectory(const std::string & path) const
{
    return disk->existsFileOrDirectory(path);
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
    return getTotalSize(readMetadata(path)->objects);
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
    auto buf = disk->readFile(path, ReadSettings{});
    std::string result;
    readStringUntilEOF(result, *buf);
    return result;
}

std::string MetadataStorageFromDisk::readInlineDataToString(const std::string & path) const
{
    return readMetadata(path)->inline_data;
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
        metadata->ref_count = 0;
        WriteBufferFromOwnString buf;
        metadata->serialize(buf);
        metadatas[path] = buf.str();
    }

    return metadatas;
}

MetadataTransactionPtr MetadataStorageFromDisk::createTransaction()
{
    return std::make_shared<MetadataStorageFromDiskTransaction>(*this);
}

bool MetadataStorageFromDisk::supportWritingWithAppend() const
{
    return true;
}

StoredObjects MetadataStorageFromDisk::getStorageObjects(const std::string & path) const
{
    return readMetadata(path)->objects;
}

uint32_t MetadataStorageFromDisk::getHardlinkCount(const std::string & path) const
{
    return readMetadata(path)->ref_count;
}

IMetadataStorage::BlobsToRemove MetadataStorageFromDisk::getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    std::lock_guard guard(removed_objects_mutex);

    if (max_count == 0)
        max_count = std::numeric_limits<int64_t>::max();

    BlobsToRemove blobs_to_remove;
    for (const auto & blob : objects_to_remove | std::views::take(max_count))
        blobs_to_remove[blob] = {cluster->getLocalLocation()};

    return blobs_to_remove;
}

int64_t MetadataStorageFromDisk::recordAsRemoved(const StoredObjects & blobs)
{
    std::lock_guard guard(removed_objects_mutex);

    int64_t recorded_count = 0;
    for (const auto & removed_blob : blobs)
        recorded_count += objects_to_remove.erase(removed_blob);

    return recorded_count;
}

MetadataStorageFromDiskTransaction::MetadataStorageFromDiskTransaction(MetadataStorageFromDisk & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

void MetadataStorageFromDiskTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);
        operations.commit();
    }

    operations.finalize();

    {
        std::lock_guard guard(metadata_storage.removed_objects_mutex);
        metadata_storage.objects_to_remove.insert_range(objects_to_remove);
    }
}

TransactionCommitOutcomeVariant MetadataStorageFromDiskTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata storage from disk supports only tryCommit without options");

    commit(NoCommitOptions{});
    return true;
}

void MetadataStorageFromDiskTransaction::writeStringToFile(
     const std::string & path,
     const std::string & data)
{
    operations.addOperation(std::make_unique<WriteFileOperation>(path, data, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::writeInlineDataToFile(
     const std::string & path,
     const std::string & data)
{
    operations.addOperation(std::make_unique<WriteInlineDataOperation>(path, data, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations.addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::chmod(const String & path, mode_t mode)
{
    operations.addOperation(std::make_unique<ChmodOperation>(path, mode, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    operations.addOperation(std::make_unique<UnlinkFileOperation>(path, if_exists, should_remove_objects, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    operations.addOperation(std::make_unique<RemoveRecursiveOperation>(path, should_remove_objects, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, metadata_storage.compatible_key_prefix, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::createDirectory(const std::string & path)
{
    operations.addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createDirectoryRecursive(const std::string & path)
{
    operations.addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::removeDirectory(const std::string & path)
{
    operations.addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::setReadOnly(const std::string & path)
{
    operations.addOperation(std::make_unique<SetReadonlyFileOperation>(path, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    operations.addOperation(std::make_unique<RewriteFileOperation>(path, objects, metadata_storage.compatible_key_prefix, *metadata_storage.disk, objects_to_remove));
}

void MetadataStorageFromDiskTransaction::addBlobToMetadata(const std::string & path, const StoredObject & object)
{
    operations.addOperation(std::make_unique<AddBlobOperation>(path, object, metadata_storage.compatible_key_prefix, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::truncateFile(const std::string & src_path, size_t target_size)
{
    operations.addOperation(std::make_unique<TruncateMetadataFileOperation>(src_path, target_size, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

ObjectStorageKey MetadataStorageFromDiskTransaction::generateObjectKeyForPath(const std::string & /*path*/)
{
    return metadata_storage.key_generator->generate();
}

StoredObjects MetadataStorageFromDiskTransaction::getSubmittedForRemovalBlobs()
{
    return objects_to_remove;
}

}
