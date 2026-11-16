#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>


namespace DB
{

MetadataStorageFromCacheObjectStorage::MetadataStorageFromCacheObjectStorage(MetadataStoragePtr underlying_, ObjectStoragePtr object_storage_)
    : underlying(std::move(underlying_))
    , object_storage(std::move(object_storage_))
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

bool MetadataStorageFromCacheObjectStorage::supportsInlineData() const
{
    return underlying->supportsInlineData();
}

bool MetadataStorageFromCacheObjectStorage::appliesOperationsEagerly() const
{
    return underlying->appliesOperationsEagerly();
}

bool MetadataStorageFromCacheObjectStorage::areBlobPathsRandom() const
{
    return underlying->areBlobPathsRandom();
}

ObjectStorageKeyGeneratorPtr MetadataStorageFromCacheObjectStorage::getKeyGenerator() const
{
    return underlying->getKeyGenerator();
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

bool MetadataStorageFromCacheObjectStorage::isRemote() const
{
    return underlying->isRemote();
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

bool MetadataStorageFromCacheObjectStorage::supportsHardLinks() const
{
    return underlying->supportsHardLinks();
}

bool MetadataStorageFromCacheObjectStorage::supportWritingWithAppend() const
{
    return underlying->supportWritingWithAppend();
}

IMetadataStorage::BlobsToRemove MetadataStorageFromCacheObjectStorage::getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    std::lock_guard guard(removed_objects_mutex);

    BlobsToRemove blobs_to_remove;
    for (const auto & blob : objects_to_remove.takeFirst(max_count))
        blobs_to_remove[blob] = {cluster->getLocalLocation()};

    return blobs_to_remove;
}

int64_t MetadataStorageFromCacheObjectStorage::recordAsRemoved(const StoredObjects & blobs)
{
    std::lock_guard guard(removed_objects_mutex);
    return objects_to_remove.markAsRemoved(blobs);
}

bool MetadataStorageFromCacheObjectStorage::hasPendingRemovalBlobs(const StoredObjects & blobs) const
{
    if (underlying->hasPendingRemovalBlobs(blobs))
        return true;

    std::lock_guard guard(removed_objects_mutex);
    return objects_to_remove.containsAny(blobs);
}

int64_t MetadataStorageFromCacheObjectStorage::getDeadBlobsQueueEstimate()
{
    std::lock_guard guard(removed_objects_mutex);
    return std::ssize(objects_to_remove);
}

bool MetadataStorageFromCacheObjectStorage::hasDeadBlobsQueue() const
{
    /// This must not be delegated to the wrapped storage: this wrapper has a queue of its own. Every commit
    /// moves the blobs that the wrapped storage removed into `objects_to_remove`, so that the blob killer
    /// drops the stale cache entries for them. That is exactly what a cached `plain` or `plain_rewritable`
    /// disk needs, where blob paths are not random and a new file reuses the path of a deleted one.
    /// The checks below only tell whether new blobs can be queued from now on. The disk may have been
    /// reloaded as read-only after some blobs were already queued, and the queue is kept only in memory,
    /// so as long as it is not empty, it has to be drained, otherwise the stale cache entries would survive.
    {
        std::lock_guard guard(removed_objects_mutex);
        if (objects_to_remove.size() > 0)
            return true;
    }

    /// A read-only storage never removes anything, so nothing is ever queued for it.
    if (underlying->isReadOnly())
        return false;

    /// `plain` and `plain_rewritable` answer `isReadOnly` with `false` even over a read-only object storage.
    /// A storage that removes its blobs synchronously has to write to the object storage for that, so over
    /// a read-only one it never removes anything either. A storage with a deferred removal queue of its own
    /// (`local`) may still commit a removal, so the queue is kept for it.
    /// The object storage is asked every time: its `readonly` setting can change on config reload.
    /// `BlobKillerThread` re-evaluates this after every round and after the object storage got the new
    /// settings, and a commit that is still in flight during the reload wakes it up in `submitForRemoval`.
    if (object_storage->isReadOnly() && !underlying->hasDeadBlobsQueue())
        return false;

    return true;
}

void MetadataStorageFromCacheObjectStorage::setDeadBlobsQueuedCallback(std::function<void()> callback)
{
    std::lock_guard guard(dead_blobs_queued_callback_mutex);
    dead_blobs_queued_callback = std::move(callback);
}

void MetadataStorageFromCacheObjectStorage::submitForRemoval(const StoredObjects & blobs)
{
    if (blobs.empty())
        return;

    {
        std::lock_guard guard(removed_objects_mutex);
        objects_to_remove.submitForRemoval(blobs);
    }

    /// Called after the blobs are queued, so `hasDeadBlobsQueue` already answers `true` in the woken up round.
    /// The mutex is held during the call, so that the callback is not unregistered while it is running.
    std::lock_guard guard(dead_blobs_queued_callback_mutex);
    if (dead_blobs_queued_callback)
        dead_blobs_queued_callback();
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

bool MetadataStorageFromCacheObjectStorage::hasMissingBlobsQueue() const
{
    return underlying->hasMissingBlobsQueue();
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
    metadata_storage.submitForRemoval(underlying->getSubmittedForRemovalBlobs());
}

TransactionCommitOutcomeVariant MetadataStorageFromCacheObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    auto result = underlying->tryCommit(options);

    if (isSuccessfulOutcome(result))
        metadata_storage.submitForRemoval(underlying->getSubmittedForRemovalBlobs());

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

void MetadataStorageFromCacheObjectStorageTransaction::incrementBlobRefCount(const std::string & blob)
{
    underlying->incrementBlobRefCount(blob);
}

void MetadataStorageFromCacheObjectStorageTransaction::decrementBlobRefCount(const std::string & blob)
{
    underlying->decrementBlobRefCount(blob);
}

void MetadataStorageFromCacheObjectStorageTransaction::submitBlobForRemoval(const std::string & remote_path)
{
    underlying->submitBlobForRemoval(remote_path);
}

}
