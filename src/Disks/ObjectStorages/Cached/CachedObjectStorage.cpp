#include "CachedObjectStorage.h"

#include <IO/BoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace FileCacheSetting
{
    extern const FileCacheSettingsBool cache_on_write_operations;
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
    , log(getLogger(getName()))
{
    cache->initialize();
}

FileCache::Key CachedObjectStorage::getCacheKey(const std::string & path) const
{
    return FileCacheKey::fromPath(path);
}

ObjectStorageKey
CachedObjectStorage::generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const
{
    return object_storage->generateObjectKeyForPath(path, key_prefix);
}

ObjectStorageKey
CachedObjectStorage::generateObjectKeyPrefixForDirectoryPath(const std::string & path, const std::optional<std::string> & key_prefix) const
{
    return object_storage->generateObjectKeyPrefixForDirectoryPath(path, key_prefix);
}

bool CachedObjectStorage::areObjectKeysRandom() const
{
    return object_storage->areObjectKeysRandom();
}

ReadSettings CachedObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    return object_storage->patchSettings(read_settings);
}

void CachedObjectStorage::startup()
{
    object_storage->startup();
}

bool CachedObjectStorage::exists(const StoredObject & object) const
{
    return object_storage->exists(object);
}

std::unique_ptr<ReadBufferFromFileBase> CachedObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (read_settings.enable_filesystem_cache)
    {
        if (cache->isInitialized())
        {
            auto cache_key = FileCacheKey::fromPath(object.remote_path);
            auto global_context = Context::getGlobalContextInstance();
            auto modified_read_settings = read_settings.withNestedBuffer();

            auto read_buffer_creator = [=, this]()
            {
                return object_storage->readObject(object, patchSettings(read_settings), read_hint, file_size);
            };

            return std::make_unique<CachedOnDiskReadBufferFromFile>(
                object.remote_path,
                cache_key,
                cache,
                FileCache::getCommonUser(),
                read_buffer_creator,
                modified_read_settings,
                std::string(CurrentThread::getQueryId()),
                object.bytes_size,
                /* allow_seeks */!read_settings.remote_read_buffer_restrict_seek,
                /* use_external_buffer */read_settings.remote_read_buffer_use_external_buffer,
                /* read_until_position */std::nullopt,
                global_context->getFilesystemCacheLog());
        }
        else
        {
            cache->throwInitExceptionIfNeeded();
        }
    }

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
        && FileCacheFactory::instance().getByName(cache_config_name)->getSettings()[FileCacheSetting::cache_on_write_operations]
        && fs::path(object.remote_path).extension() != ".tmp";

    /// Need to remove even if cache_on_write == false.
    removeCacheIfExists(object.remote_path);

    if (cache_on_write && cache->isInitialized())
    {
        auto key = getCacheKey(object.remote_path);
        return std::make_unique<CachedOnDiskWriteBufferFromFile>(
            std::move(implementation_buffer),
            cache,
            implementation_buffer->getFileName(),
            key,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? std::string(CurrentThread::getQueryId()) : "",
            modified_write_settings,
            FileCache::getCommonUser(),
            Context::getGlobalContextInstance()->getFilesystemCacheLog());
    }

    return implementation_buffer;
}

void CachedObjectStorage::removeCacheIfExists(const std::string & path_key_for_cache)
{
    if (path_key_for_cache.empty())
        return;

    /// Add try catch?
    if (cache->isInitialized())
        cache->removeKeyIfExists(getCacheKey(path_key_for_cache), FileCache::getCommonUser().user_id);
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

void CachedObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    object_storage->copyObjectToAnotherObjectStorage(object_from, object_to, read_settings, write_settings, object_storage_to, object_to_attributes);
}

void CachedObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes)
{
    object_storage->copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
}

void CachedObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    object_storage->listObjects(path, children, max_keys);
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
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    ContextPtr context, const ApplyNewSettingsOptions & options)
{
    object_storage->applyNewSettings(config, config_prefix, context, options);
}

String CachedObjectStorage::getObjectsNamespace() const
{
    return object_storage->getObjectsNamespace();
}

}
