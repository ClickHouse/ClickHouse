#include "CachedObjectStorage.h"

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <IO/BoundedReadBuffer.h>
#include <Disks/IO/CachedWriteBufferFromFile.h>
#include <Disks/IO/CachedReadBufferFromFile.h>
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
    extern const int CANNOT_STAT;
}

CachedObjectStorage:: CachedObjectStorage(ObjectStoragePtr object_storage_, FileCachePtr cache_)
    : object_storage(object_storage_)
    , cache(cache_)
    , log(&Poco::Logger::get(getName() + "(" + object_storage_->getName() +")"))
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

ReadSettings CachedObjectStorage::getReadSettingsForCache(const ReadSettings & read_settings) const
{
    ReadSettings result_settings{read_settings};
    result_settings.remote_fs_cache = cache;

    if (IFileCache::isReadOnly())
        result_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

    return result_settings;
}

void CachedObjectStorage::startup()
{
    object_storage->startup();
}

bool CachedObjectStorage::exists(const StoredObject & object) const
{
    fs::path cache_path = getCachePath(object.getPathKeyForCache());

    if (fs::exists(cache_path) && !cache_path.empty())
        return true;

    return object_storage->exists(object);
}

std::unique_ptr<ReadBufferFromFileBase> CachedObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    assert(!objects[0].getPathKeyForCache().empty());

    auto modified_read_settings = getReadSettingsForCache(read_settings);
    auto impl = object_storage->readObjects(objects, modified_read_settings, read_hint, file_size);

    /// If underlying read buffer does caching on its own, do not wrap it in caching buffer.
    if (impl->isIntegratedWithFilesystemCache()
        && modified_read_settings.enable_filesystem_cache_on_lower_level)
    {
        return impl;
    }
    else
    {
        if (!file_size)
            file_size = impl->getFileSize();

        auto implementation_buffer_creator = [=, this]()
        {
            auto implementation_buffer =
                object_storage->readObjects(objects, modified_read_settings, read_hint, file_size);
            return std::make_unique<BoundedReadBuffer>(std::move(implementation_buffer));
        };

        if (objects.size() != 1)
            throw Exception(ErrorCodes::CANNOT_USE_CACHE, "Unable to read multiple objects, support not added");

        std::string path = objects[0].absolute_path;
        IFileCache::Key key = getCacheKey(objects[0].getPathKeyForCache());

        return std::make_unique<CachedReadBufferFromFile>(
            path,
            key,
            cache,
            implementation_buffer_creator,
            modified_read_settings,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? CurrentThread::getQueryId().toString() : "",
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
    auto modified_read_settings = getReadSettingsForCache(read_settings);
    auto impl = object_storage->readObject(object, read_settings, read_hint, file_size);

    /// If underlying read buffer does caching on its own, do not wrap it in caching buffer.
    if (impl->isIntegratedWithFilesystemCache()
        && modified_read_settings.enable_filesystem_cache_on_lower_level)
    {
        return impl;
    }
    else
    {
        if (!file_size)
            file_size = impl->getFileSize();

        auto implementation_buffer_creator = [=, this]()
        {
            auto implementation_buffer =
                object_storage->readObject(object, read_settings, read_hint, file_size);
            return std::make_unique<BoundedReadBuffer>(std::move(implementation_buffer));
        };

        IFileCache::Key key = getCacheKey(object.getPathKeyForCache());
        LOG_TEST(log, "Reading from file `{}` with cache key `{}`", object.absolute_path, key.toString());
        return std::make_unique<CachedReadBufferFromFile>(
            object.absolute_path,
            key,
            cache,
            implementation_buffer_creator,
            read_settings,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? CurrentThread::getQueryId().toString() : "",
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
    auto impl = object_storage->writeObject(object, mode, attributes, std::move(finalize_callback), buf_size, write_settings);

    bool cache_on_write = fs::path(object.absolute_path).extension() != ".tmp"
        && write_settings.enable_filesystem_cache_on_write_operations
        && FileCacheFactory::instance().getSettings(cache->getBasePath()).cache_on_write_operations;

    auto cache_hint = object.getPathKeyForCache();
    removeCacheIfExists(cache_hint);

    if (cache_on_write)
    {
        auto key = getCacheKey(cache_hint);
        LOG_TEST(log, "Caching file `{}` to `{}` with key {}", object.absolute_path, getCachePath(cache_hint), key.toString());

        return std::make_unique<CachedWriteBufferFromFile>(
            std::move(impl),
            cache,
            impl->getFileName(),
            key,
            write_settings.is_file_cache_persistent,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? CurrentThread::getQueryId().toString() : "",
            write_settings);
    }

    return impl;
}

void CachedObjectStorage::removeCacheIfExists(const std::string & cache_hint)
{
    if (cache_hint.empty())
        return;

    IFileCache::Key key;
    try
    {
        key = getCacheKey(cache_hint);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_STAT)
            return;
        throw;
    }
    cache->removeIfExists(key);
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
