#include "CachedObjectStorage.h"

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <IO/BoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CachedObjectStorage::CachedObjectStorage(
    ObjectStoragePtr object_storage_,
    FileCachePtr cache_,
    const FileCacheSettings & cache_settings_,
    const std::string & cache_config_name_)
    : object_storage(object_storage_)
    , cache(cache_)
    , cache_settings(cache_settings_)
    , cache_config_name(cache_config_name_)
    , log(&Poco::Logger::get(getName()))
{
    cache->initialize();
}

DataSourceDescription CachedObjectStorage::getDataSourceDescription() const
{
    auto wrapped_object_storage_data_source = object_storage->getDataSourceDescription();
    wrapped_object_storage_data_source.is_cached = true;
    return wrapped_object_storage_data_source;
}

FileCache::Key CachedObjectStorage::getCacheKey(const std::string & path) const
{
    return cache->createKeyForPath(path);
}

std::string CachedObjectStorage::generateBlobNameForPath(const std::string & path)
{
    return object_storage->generateBlobNameForPath(path);
}

ReadSettings CachedObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    ReadSettings modified_settings{read_settings};
    modified_settings.remote_fs_cache = cache;

    if (!canUseReadThroughCache(read_settings))
        modified_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

    return object_storage->patchSettings(modified_settings);
}

void CachedObjectStorage::startup()
{
    object_storage->startup();
}

bool CachedObjectStorage::exists(const StoredObject & object) const
{
    return object_storage->exists(object);
}

std::unique_ptr<ReadBufferFromFileBase> CachedObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (objects.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Received empty list of objects to read");
    return object_storage->readObjects(objects, patchSettings(read_settings), read_hint, file_size);
}

std::unique_ptr<ReadBufferFromFileBase> CachedObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    return object_storage->readObject(object, patchSettings(read_settings), read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> CachedObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode, // Cached doesn't support append, only rewrite
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    /// Add cache relating settings to WriteSettings.
    auto modified_write_settings = IObjectStorage::patchSettings(write_settings);
    auto implementation_buffer = object_storage->writeObject(object, mode, attributes, buf_size, modified_write_settings);

    bool cache_on_write = modified_write_settings.enable_filesystem_cache_on_write_operations
        && FileCacheFactory::instance().getByName(cache_config_name).settings.cache_on_write_operations
        && fs::path(object.remote_path).extension() != ".tmp";

    /// Need to remove even if cache_on_write == false.
    removeCacheIfExists(object.remote_path);

    if (cache_on_write)
    {
        auto key = getCacheKey(object.remote_path);
        return std::make_unique<CachedOnDiskWriteBufferFromFile>(
            std::move(implementation_buffer),
            cache,
            implementation_buffer->getFileName(),
            key,
            modified_write_settings.is_file_cache_persistent,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? std::string(CurrentThread::getQueryId()) : "",
            modified_write_settings);
    }

    return implementation_buffer;
}

void CachedObjectStorage::removeCacheIfExists(const std::string & path_key_for_cache)
{
    if (path_key_for_cache.empty())
        return;

    /// Add try catch?
    cache->removeKeyIfExists(getCacheKey(path_key_for_cache));
}

void CachedObjectStorage::removeObject(const StoredObject & object)
{
    object_storage->removeObject(object);
}

void CachedObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeCacheIfExists(object.remote_path);

    object_storage->removeObjects(objects);
}

void CachedObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeCacheIfExists(object.remote_path);
    object_storage->removeObjectIfExists(object);
}

void CachedObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeCacheIfExists(object.remote_path);

    object_storage->removeObjectsIfExist(objects);
}

ReadSettings CachedObjectStorage::getAdjustedSettingsFromMetadataFile(const ReadSettings & settings, const std::string & path) const
{
    ReadSettings new_settings{settings};
    new_settings.is_file_cache_persistent = isFileWithPersistentCache(path) && cache_settings.do_not_evict_index_and_mark_files;
    return new_settings;
}

WriteSettings CachedObjectStorage::getAdjustedSettingsFromMetadataFile(const WriteSettings & settings, const std::string & path) const
{
    WriteSettings new_settings{settings};
    new_settings.is_file_cache_persistent = isFileWithPersistentCache(path) && cache_settings.do_not_evict_index_and_mark_files;
    return new_settings;
}

void CachedObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    object_storage->copyObjectToAnotherObjectStorage(object_from, object_to, object_storage_to, object_to_attributes);
}

void CachedObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from, const StoredObject & object_to, std::optional<ObjectAttributes> object_to_attributes)
{
    object_storage->copyObject(object_from, object_to, object_to_attributes);
}

std::unique_ptr<IObjectStorage> CachedObjectStorage::cloneObjectStorage(
    const std::string & new_namespace,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context)
{
    return object_storage->cloneObjectStorage(new_namespace, config, config_prefix, context);
}

void CachedObjectStorage::findAllFiles(const std::string & path, RelativePathsWithSize & children, int max_keys) const
{
    object_storage->findAllFiles(path, children, max_keys);
}

ObjectMetadata CachedObjectStorage::getObjectMetadata(const std::string & path) const
{
    return object_storage->getObjectMetadata(path);
}

void CachedObjectStorage::shutdown()
{
    object_storage->shutdown();
}

void CachedObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    object_storage->applyNewSettings(config, config_prefix, context);
}

String CachedObjectStorage::getObjectsNamespace() const
{
    return object_storage->getObjectsNamespace();
}

bool CachedObjectStorage::canUseReadThroughCache(const ReadSettings & settings)
{
    if (!settings.avoid_readthrough_cache_outside_query_context)
        return true;

    return CurrentThread::isInitialized()
        && CurrentThread::get().getQueryContext()
        && !CurrentThread::getQueryId().empty();
}

}
