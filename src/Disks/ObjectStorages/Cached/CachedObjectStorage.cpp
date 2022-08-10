#include "CachedObjectStorage.h"

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <IO/BoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Common/IFileCache.h>
#include <Common/FileCacheFactory.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_USE_CACHE;
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

IFileCache::Key CachedObjectStorage::getCacheKey(const std::string & path) const
{
    return cache->hash(path);
}

String CachedObjectStorage::getCachePath(const std::string & path) const
{
    IFileCache::Key cache_key = getCacheKey(path);
    return cache->getPathInLocalCache(cache_key);
}

std::string CachedObjectStorage::generateBlobNameForPath(const std::string & path)
{
    return object_storage->generateBlobNameForPath(path);
}

ReadSettings CachedObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    ReadSettings modified_settings{read_settings};
    modified_settings.remote_fs_cache = cache;

    if (IFileCache::isReadOnly())
        modified_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

    return IObjectStorage::patchSettings(modified_settings);
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
    assert(!objects[0].getPathKeyForCache().empty());

    auto modified_read_settings = patchSettings(read_settings);
    auto implementation_buffer = object_storage->readObjects(objects, modified_read_settings, read_hint, file_size);

    /// If underlying read buffer does caching on its own, do not wrap it in caching buffer.
    if (implementation_buffer->isIntegratedWithFilesystemCache()
        && modified_read_settings.enable_filesystem_cache_on_lower_level)
    {
        return implementation_buffer;
    }
    else
    {
        if (!file_size)
            file_size = implementation_buffer->getFileSize();

        auto implementation_buffer_creator = [objects, modified_read_settings, read_hint, file_size, this]()
        {
            return std::make_unique<BoundedReadBuffer>(
                object_storage->readObjects(objects, modified_read_settings, read_hint, file_size));
        };

        if (objects.size() != 1)
            throw Exception(ErrorCodes::CANNOT_USE_CACHE, "Unable to read multiple objects, support not added");

        std::string path = objects[0].absolute_path;
        IFileCache::Key key = getCacheKey(objects[0].getPathKeyForCache());

        return std::make_unique<CachedOnDiskReadBufferFromFile>(
            path,
            key,
            cache,
            implementation_buffer_creator,
            modified_read_settings,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? std::string(CurrentThread::getQueryId()) : "",
            file_size.value(),
            /* allow_seeks */true,
            /* use_external_buffer */false);
    }
}

std::unique_ptr<ReadBufferFromFileBase> CachedObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    auto modified_read_settings = patchSettings(read_settings);
    auto implementation_buffer = object_storage->readObject(object, read_settings, read_hint, file_size);

    /// If underlying read buffer does caching on its own, do not wrap it in caching buffer.
    if (implementation_buffer->isIntegratedWithFilesystemCache()
        && modified_read_settings.enable_filesystem_cache_on_lower_level)
    {
        return implementation_buffer;
    }
    else
    {
        if (!file_size)
            file_size = implementation_buffer->getFileSize();

        auto implementation_buffer_creator = [object, read_settings, read_hint, file_size, this]()
        {
            return std::make_unique<BoundedReadBuffer>(object_storage->readObject(object, read_settings, read_hint, file_size));
        };

        IFileCache::Key key = getCacheKey(object.getPathKeyForCache());
        LOG_TEST(log, "Reading from file `{}` with cache key `{}`", object.absolute_path, key.toString());
        return std::make_unique<CachedOnDiskReadBufferFromFile>(
            object.absolute_path,
            key,
            cache,
            implementation_buffer_creator,
            read_settings,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? std::string(CurrentThread::getQueryId()) : "",
            file_size.value(),
            /* allow_seeks */true,
            /* use_external_buffer */false);
    }
}


std::unique_ptr<WriteBufferFromFileBase> CachedObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode, // Cached doesn't support append, only rewrite
    std::optional<ObjectAttributes> attributes,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    auto modified_write_settings = IObjectStorage::patchSettings(write_settings);
    auto implementation_buffer = object_storage->writeObject(object, mode, attributes, std::move(finalize_callback), buf_size, modified_write_settings);

    bool cache_on_write = fs::path(object.absolute_path).extension() != ".tmp"
        && modified_write_settings.enable_filesystem_cache_on_write_operations
        && FileCacheFactory::instance().getSettings(cache->getBasePath()).cache_on_write_operations;

    auto path_key_for_cache = object.getPathKeyForCache();
    /// Need to remove even if cache_on_write == false.
    removeCacheIfExists(path_key_for_cache);

    if (cache_on_write)
    {
        auto key = getCacheKey(path_key_for_cache);
        LOG_TEST(log, "Caching file `{}` to `{}` with key {}", object.absolute_path, getCachePath(path_key_for_cache), key.toString());

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
    cache->removeIfExists(getCacheKey(path_key_for_cache));
}

void CachedObjectStorage::removeObject(const StoredObject & object)
{
    removeCacheIfExists(object.getPathKeyForCache());
    object_storage->removeObject(object);
}

void CachedObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeCacheIfExists(object.getPathKeyForCache());

    object_storage->removeObjects(objects);
}

void CachedObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeCacheIfExists(object.getPathKeyForCache());
    object_storage->removeObjectIfExists(object);
}

void CachedObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeCacheIfExists(object.getPathKeyForCache());

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

void CachedObjectStorage::listPrefix(const std::string & path, RelativePathsWithSize & children) const
{
    object_storage->listPrefix(path, children);
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

}
