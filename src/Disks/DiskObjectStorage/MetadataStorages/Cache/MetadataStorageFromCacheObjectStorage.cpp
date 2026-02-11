#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>

#include <limits>
#include <ranges>

namespace DB
{

MetadataStorageFromCacheObjectStorage::MetadataStorageFromCacheObjectStorage(MetadataStoragePtr underlying_)
    : underlying(std::move(underlying_))
{
}

MetadataStoragePtr MetadataStorageFromCacheObjectStorage::getUnderlying() const
{
    return underlying;
}

MetadataTransactionPtr MetadataStorageFromCacheObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromCacheObjectStorageTransaction>(underlying->createTransaction(), *this);
}

const std::string & MetadataStorageFromCacheObjectStorage::getPath() const
{
    return underlying->getPath();
}

MetadataStorageType MetadataStorageFromCacheObjectStorage::getType() const
{
    return underlying->getType();
}

std::string MetadataStorageFromCacheObjectStorage::getZooKeeperName() const
{
    return underlying->getZooKeeperName();
}

std::string MetadataStorageFromCacheObjectStorage::getZooKeeperPath() const
{
    return underlying->getZooKeeperPath();
}

bool MetadataStorageFromCacheObjectStorage::supportsEmptyFilesWithoutBlobs() const
{
    return underlying->supportsEmptyFilesWithoutBlobs();
}

bool MetadataStorageFromCacheObjectStorage::areBlobPathsRandom() const
{
    return underlying->areBlobPathsRandom();
}

bool MetadataStorageFromCacheObjectStorage::existsFile(const std::string & path) const
{
    return underlying->existsFile(path);
}

bool MetadataStorageFromCacheObjectStorage::existsDirectory(const std::string & path) const
{
    return underlying->existsDirectory(path);
}

bool MetadataStorageFromCacheObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    return underlying->existsFileOrDirectory(path);
}

uint64_t MetadataStorageFromCacheObjectStorage::getFileSize(const std::string & path) const
{
    return underlying->getFileSize(path);
}

std::optional<uint64_t> MetadataStorageFromCacheObjectStorage::getFileSizeIfExists(const std::string & path) const
{
    return underlying->getFileSizeIfExists(path);
}

Poco::Timestamp MetadataStorageFromCacheObjectStorage::getLastModified(const std::string & path) const
{
    return underlying->getLastModified(path);
}

std::optional<Poco::Timestamp> MetadataStorageFromCacheObjectStorage::getLastModifiedIfExists(const std::string & path) const
{
    return underlying->getLastModifiedIfExists(path);
}

time_t MetadataStorageFromCacheObjectStorage::getLastChanged(const std::string & path) const
{
    return underlying->getLastChanged(path);
}

bool MetadataStorageFromCacheObjectStorage::supportsChmod() const
{
    return underlying->supportsChmod();
}

bool MetadataStorageFromCacheObjectStorage::supportsStat() const
{
    return underlying->supportsStat();
}

struct stat MetadataStorageFromCacheObjectStorage::stat(const String & path) const
{
    return underlying->stat(path);
}

std::vector<std::string> MetadataStorageFromCacheObjectStorage::listDirectory(const std::string & path) const
{
    return underlying->listDirectory(path);
}

DirectoryIteratorPtr MetadataStorageFromCacheObjectStorage::iterateDirectory(const std::string & path) const
{
    return underlying->iterateDirectory(path);
}

bool MetadataStorageFromCacheObjectStorage::isDirectoryEmpty(const std::string & path) const
{
    return underlying->isDirectoryEmpty(path);
}

uint32_t MetadataStorageFromCacheObjectStorage::getHardlinkCount(const std::string & path) const
{
    return underlying->getHardlinkCount(path);
}

std::string MetadataStorageFromCacheObjectStorage::readFileToString(const std::string & path) const
{
    return underlying->readFileToString(path);
}

std::string MetadataStorageFromCacheObjectStorage::readInlineDataToString(const std::string & path) const
{
    return underlying->readInlineDataToString(path);
}

void MetadataStorageFromCacheObjectStorage::startup()
{
    underlying->startup();
}

void MetadataStorageFromCacheObjectStorage::shutdown()
{
    underlying->shutdown();
}

void MetadataStorageFromCacheObjectStorage::refresh(UInt64 not_sooner_than_milliseconds)
{
    underlying->refresh(not_sooner_than_milliseconds);
}

std::unordered_map<std::string, std::string> MetadataStorageFromCacheObjectStorage::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    return underlying->getSerializedMetadata(file_paths);
}

StoredObjects MetadataStorageFromCacheObjectStorage::getStorageObjects(const std::string & path) const
{
    return underlying->getStorageObjects(path);
}

std::optional<StoredObjects> MetadataStorageFromCacheObjectStorage::getStorageObjectsIfExist(const std::string & path) const
{
    return underlying->getStorageObjectsIfExist(path);
}

bool MetadataStorageFromCacheObjectStorage::isReadOnly() const
{
    return underlying->isReadOnly();
}

bool MetadataStorageFromCacheObjectStorage::isTransactional() const
{
    return underlying->isTransactional();
}

bool MetadataStorageFromCacheObjectStorage::isPlain() const
{
    return underlying->isPlain();
}

bool MetadataStorageFromCacheObjectStorage::isWriteOnce() const
{
    return underlying->isWriteOnce();
}

bool MetadataStorageFromCacheObjectStorage::supportWritingWithAppend() const
{
    return underlying->supportWritingWithAppend();
}

IMetadataStorage::BlobsToRemove MetadataStorageFromCacheObjectStorage::getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    std::lock_guard guard(removed_objects_mutex);

    if (max_count == 0)
        max_count = std::numeric_limits<int64_t>::max();

    BlobsToRemove blobs_to_remove;
    for (const auto & blob : objects_to_remove | std::views::take(max_count))
        blobs_to_remove[blob] = {cluster->getLocalLocation()};

    return blobs_to_remove;
}

int64_t MetadataStorageFromCacheObjectStorage::recordAsRemoved(const StoredObjects & blobs)
{
    std::lock_guard guard(removed_objects_mutex);

    int64_t recorded_count = 0;
    for (const auto & removed_blob : blobs)
        recorded_count += objects_to_remove.erase(removed_blob);

    return recorded_count;
}

IMetadataStorage::BlobsToReplicate MetadataStorageFromCacheObjectStorage::getBlobsToReplicate(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    return underlying->getBlobsToReplicate(cluster, max_count);
}

int64_t MetadataStorageFromCacheObjectStorage::recordAsReplicated(const BlobsToReplicate & blobs)
{
    return underlying->recordAsReplicated(blobs);
}

bool MetadataStorageFromCacheObjectStorage::hasUnreplicatedBlobs(const Location & location_to_check)
{
    return underlying->hasUnreplicatedBlobs(location_to_check);
}

void MetadataStorageFromCacheObjectStorage::updateCache(const std::vector<std::string> & paths, bool recursive, bool enforce_fresh, std::string * serialized_cache_update_description)
{
    underlying->updateCache(paths, recursive, enforce_fresh, serialized_cache_update_description);
}

void MetadataStorageFromCacheObjectStorage::updateCacheFromSerializedDescription(const std::string & serialized_cache_update_description)
{
    underlying->updateCacheFromSerializedDescription(serialized_cache_update_description);
}

void MetadataStorageFromCacheObjectStorage::invalidateCache(const std::string & path)
{
    underlying->invalidateCache(path);
}

void MetadataStorageFromCacheObjectStorage::dropCache()
{
    underlying->dropCache();
}

void MetadataStorageFromCacheObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    underlying->applyNewSettings(config, config_prefix, context);
}

MetadataStorageFromCacheObjectStorageTransaction::MetadataStorageFromCacheObjectStorageTransaction(MetadataTransactionPtr underlying_, MetadataStorageFromCacheObjectStorage & metadata_storage_)
    : underlying(std::move(underlying_))
    , metadata_storage(metadata_storage_)
{
}

void MetadataStorageFromCacheObjectStorageTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    underlying->commit(options);

    {
        std::lock_guard guard(metadata_storage.removed_objects_mutex);
        metadata_storage.objects_to_remove.insert_range(underlying->getSubmittedForRemovalBlobs());
    }
}

TransactionCommitOutcomeVariant MetadataStorageFromCacheObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    auto result = underlying->tryCommit(options);

    if (isSuccessfulOutcome(result))
    {
        std::lock_guard guard(metadata_storage.removed_objects_mutex);
        metadata_storage.objects_to_remove.insert_range(underlying->getSubmittedForRemovalBlobs());
    }

    return result;
}

void MetadataStorageFromCacheObjectStorageTransaction::writeStringToFile(const std::string & path, const std::string & data)
{
    underlying->writeStringToFile(path, data);
}

void MetadataStorageFromCacheObjectStorageTransaction::writeInlineDataToFile(const std::string & path, const std::string & data)
{
    underlying->writeInlineDataToFile(path, data);
}

void MetadataStorageFromCacheObjectStorageTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    underlying->setLastModified(path, timestamp);
}

bool MetadataStorageFromCacheObjectStorageTransaction::supportsChmod() const
{
    return underlying->supportsChmod();
}

void MetadataStorageFromCacheObjectStorageTransaction::chmod(const String & path, mode_t mode)
{
    underlying->chmod(path, mode);
}

void MetadataStorageFromCacheObjectStorageTransaction::setReadOnly(const std::string & path)
{
    underlying->setReadOnly(path);
}

void MetadataStorageFromCacheObjectStorageTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    underlying->unlinkFile(path, if_exists, should_remove_objects);
}

void MetadataStorageFromCacheObjectStorageTransaction::createDirectory(const std::string & path)
{
    underlying->createDirectory(path);
}

void MetadataStorageFromCacheObjectStorageTransaction::createDirectoryRecursive(const std::string & path)
{
    underlying->createDirectoryRecursive(path);
}

void MetadataStorageFromCacheObjectStorageTransaction::removeDirectory(const std::string & path)
{
    underlying->removeDirectory(path);
}

void MetadataStorageFromCacheObjectStorageTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    underlying->removeRecursive(path, should_remove_objects);
}

void MetadataStorageFromCacheObjectStorageTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    underlying->createHardLink(path_from, path_to);
}

void MetadataStorageFromCacheObjectStorageTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    underlying->moveFile(path_from, path_to);
}

void MetadataStorageFromCacheObjectStorageTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    underlying->moveDirectory(path_from, path_to);
}

void MetadataStorageFromCacheObjectStorageTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    underlying->replaceFile(path_from, path_to);
}

ObjectStorageKey MetadataStorageFromCacheObjectStorageTransaction::generateObjectKeyForPath(const std::string & path)
{
    return underlying->generateObjectKeyForPath(path);
}

void MetadataStorageFromCacheObjectStorageTransaction::recordBlobsReplication(const StoredObject & blob, const Locations & missing_locations)
{
    underlying->recordBlobsReplication(blob, missing_locations);
}

StoredObjects MetadataStorageFromCacheObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return underlying->getSubmittedForRemovalBlobs();
}

void MetadataStorageFromCacheObjectStorageTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    underlying->createMetadataFile(path, objects);
}

void MetadataStorageFromCacheObjectStorageTransaction::addBlobToMetadata(const std::string & path, const StoredObject & object)
{
    underlying->addBlobToMetadata(path, object);
}

void MetadataStorageFromCacheObjectStorageTransaction::truncateFile(const std::string & path, size_t size)
{
    underlying->truncateFile(path, size);
}

}
