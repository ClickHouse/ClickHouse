#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/IDisk.h>

#include <Common/ObjectStorageKey.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/filesystemHelpers.h>

#include <IO/Expect404ResponseScope.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

ObjectStorageKey getKeyForPath(const std::string & common_key_prefix, const std::string & path)
{
    return ObjectStorageKey::createAsRelative(common_key_prefix, path);
}

}

MetadataStorageFromPlainObjectStorage::MetadataStorageFromPlainObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t object_metadata_cache_size)
    : object_storage(object_storage_)
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
{
    if (object_metadata_cache_size)
        object_metadata_cache = std::make_shared<CacheBase<UInt128, ObjectMetadataEntry>>(CurrentMetrics::end(), CurrentMetrics::end(), object_metadata_cache_size);
}

MetadataTransactionPtr MetadataStorageFromPlainObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainObjectStorageTransaction>(*this, object_storage);
}

bool MetadataStorageFromPlainObjectStorage::existsFile(const std::string & path) const
{
    ObjectStorageKey object_key = getKeyForPath(object_storage->getCommonKeyPrefix(), path);
    StoredObject object(object_key.serialize(), path);
    if (!object_storage->exists(object))
        return false;

    /// The path does not correspond to a directory.
    /// This check is required for a local object storage since it supports hierarchy.
    auto directory = std::filesystem::path(object_key.serialize()) / "";
    ObjectStorageKey directory_key = getKeyForPath(object_storage->getCommonKeyPrefix(), directory);
    return !object_storage->exists(StoredObject(directory_key.serialize(), directory));
}

bool MetadataStorageFromPlainObjectStorage::existsDirectory(const std::string & path) const
{
    auto key_prefix = getKeyForPath(object_storage->getCommonKeyPrefix(), path).serialize();
    auto directory = std::filesystem::path(std::move(key_prefix)) / "";
    return object_storage->existsOrHasAnyChild(directory);
}

bool MetadataStorageFromPlainObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    /// NOTE: exists() cannot be used here since it works only for existing
    /// key, and does not work for some intermediate path.
    auto key_prefix = getKeyForPath(object_storage->getCommonKeyPrefix(), path).serialize();
    return object_storage->existsOrHasAnyChild(key_prefix);
}

uint64_t MetadataStorageFromPlainObjectStorage::getFileSize(const String & path) const
{
    if (auto res = getFileSizeIfExists(path))
        return *res;
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist on {}", path, object_storage->getName());
}

std::optional<uint64_t> MetadataStorageFromPlainObjectStorage::getFileSizeIfExists(const String & path) const
{
    Expect404ResponseScope scope;  // 404 is not an error
    if (auto res = getObjectMetadataEntryWithCache(path))
        return res->file_size;
    return std::nullopt;
}

Poco::Timestamp MetadataStorageFromPlainObjectStorage::getLastModified(const std::string & path) const
{
    if (auto res = getLastModifiedIfExists(path))
        return *res;
    else
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory {} does not exist on {}", path, object_storage->getName());
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainObjectStorage::getLastModifiedIfExists(const std::string & path) const
{
    /// Since the plain object storage is used for backups only, return the current time.
    if (existsFileOrDirectory(path))
        return Poco::Timestamp{};
    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromPlainObjectStorage::listDirectory(const std::string & path) const
{
    auto key_prefix = getKeyForPath(object_storage->getCommonKeyPrefix(), path).serialize();

    RelativePathsWithMetadata files;
    std::string absolute_key = key_prefix;
    if (!absolute_key.empty() && !absolute_key.ends_with('/'))
        absolute_key += '/';

    object_storage->listObjects(absolute_key, files, 0);

    std::unordered_set<std::string> result;
    for (const auto & elem : files)
    {
        const auto & p = elem->relative_path;
        chassert(p.find(absolute_key) == 0);
        const auto child_pos = absolute_key.size();
        /// string::npos is ok.
        const auto slash_pos = p.find('/', child_pos);
        if (slash_pos == std::string::npos)
            result.emplace(p.substr(child_pos));
        else
            result.emplace(p.substr(child_pos, slash_pos - child_pos));
    }
    return std::vector<std::string>(std::make_move_iterator(result.begin()), std::make_move_iterator(result.end()));
}

DirectoryIteratorPtr MetadataStorageFromPlainObjectStorage::iterateDirectory(const std::string & path) const
{
    /// Required for MergeTree
    auto paths = listDirectory(path);

    /// Prepend path, since iterateDirectory() includes path, unlike listDirectory()
    std::for_each(paths.begin(), paths.end(), [&](auto & child) { child = fs::path(path) / child; });
    std::vector<std::filesystem::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

StoredObjects MetadataStorageFromPlainObjectStorage::getStorageObjects(const std::string & path) const
{
    size_t object_size = getFileSize(path);
    auto object_key = getKeyForPath(object_storage->getCommonKeyPrefix(), path);
    return {StoredObject(object_key.serialize(), path, object_size)};
}

std::optional<StoredObjects> MetadataStorageFromPlainObjectStorage::getStorageObjectsIfExist(const std::string & path) const
{
    if (auto object_size = getFileSizeIfExists(path))
    {
        auto object_key = getKeyForPath(object_storage->getCommonKeyPrefix(), path);
        return StoredObjects{StoredObject(object_key.serialize(), path, *object_size)};
    }
    return std::nullopt;
}

ObjectMetadataEntryPtr MetadataStorageFromPlainObjectStorage::getObjectMetadataEntryWithCache(const std::string & path) const
{
    auto object_key = getKeyForPath(object_storage->getCommonKeyPrefix(), path);
    auto get = [&] -> ObjectMetadataEntryPtr
    {
        if (auto metadata = object_storage->tryGetObjectMetadata(object_key.serialize(), /*with_tags=*/ false))
            return std::make_shared<ObjectMetadataEntry>(metadata->size_bytes, metadata->last_modified.epochTime());
        return nullptr;
    };

    if (object_metadata_cache)
    {
        SipHash hash;
        hash.update(object_key.serialize());
        auto hash128 = hash.get128();
        if (auto res = object_metadata_cache->get(hash128))
            return res;
        if (auto mapped = get())
            return object_metadata_cache->getOrSet(hash128, [&] { return mapped; }).first;
        return object_metadata_cache->get(hash128);
    }
    return get();
}

MetadataStorageFromPlainObjectStorageTransaction::MetadataStorageFromPlainObjectStorageTransaction(
    MetadataStorageFromPlainObjectStorage & metadata_storage_, ObjectStoragePtr object_storage_)
    : metadata_storage(metadata_storage_), object_storage(object_storage_)
{
}

void MetadataStorageFromPlainObjectStorageTransaction::commit(const TransactionCommitOptionsVariant &)
{
}

TransactionCommitOutcomeVariant MetadataStorageFromPlainObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant &)
{
    return true;
}

void MetadataStorageFromPlainObjectStorageTransaction::unlinkFile(const std::string & path, bool /*if_exists*/, bool /*should_remove_objects*/)
{
    if (metadata_storage.object_metadata_cache)
    {
        auto object_key = getKeyForPath(object_storage->getCommonKeyPrefix(), path);
        SipHash hash;
        hash.update(object_key.serialize());
        metadata_storage.object_metadata_cache->remove(hash.get128());
    }

    auto object_key = getKeyForPath(object_storage->getCommonKeyPrefix(), path);
    metadata_storage.object_storage->removeObjectIfExists(StoredObject(object_key.serialize()));
    objects_to_remove.push_back(StoredObject(object_key.serialize()));
}

void MetadataStorageFromPlainObjectStorageTransaction::removeDirectory(const std::string & path)
{
    for (auto it = metadata_storage.iterateDirectory(path); it->isValid(); it->next())
    {
        metadata_storage.object_storage->removeObjectIfExists(StoredObject(it->path()));
        objects_to_remove.push_back(StoredObject(it->path()));
    }
}

void MetadataStorageFromPlainObjectStorageTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & /*should_remove_objects*/)
{
    /// TODO: Implement recursive listing.
    removeDirectory(path);
}

ObjectStorageKey MetadataStorageFromPlainObjectStorageTransaction::generateObjectKeyForPath(const std::string & path)
{
    return getKeyForPath(object_storage->getCommonKeyPrefix(), path);
}

StoredObjects MetadataStorageFromPlainObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return objects_to_remove;
}

}
