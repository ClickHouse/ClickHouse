#include <Disks/ObjectStorages/CachedObjectStorage.h>

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
}


IFileCache::Key CachedObjectStorage::getCacheKey(const std::string & path) const
{
    std::string path_id = object_storage->getUniqueIdForBlob(path);
    return cache->hash(path_id);
}

String CachedObjectStorage::getCachePath(const std::string & path) const
{
    IFileCache::Key cache_key = getCacheKey(path);
    return cache->getPathInLocalCache(cache_key);
}

ReadSettings CachedObjectStorage::getReadSettingsForCache(const ReadSettings & read_settings) const
{
    ReadSettings result_settings{read_settings};
    result_settings.remote_fs_cache = cache;

    if (IFileCache::isReadOnly())
        result_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

    // if (modified_read_settings.filesystem_cache_do_not_evict_index_and_marks_files && isFilePersistent(path))
    //     read_settings.cache_file_as_persistent = true;

    return result_settings;
}

void CachedObjectStorage::startup()
{
    cache->initialize();
    ObjectStorageProxy::startup();
}

bool CachedObjectStorage::exists(const std::string & path) const
{
    fs::path cache_path = getCachePath(path);

    if (fs::exists(cache_path) && !cache_path.empty())
        return true;

    return ObjectStorageProxy::exists(path);
}

std::unique_ptr<ReadBufferFromFileBase> CachedObjectStorage::readObjects( /// NOLINT
    const std::string & common_path_prefix,
    const BlobsPathToSize & blobs_to_read,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    auto modified_read_settings = getReadSettingsForCache(read_settings);
    auto impl = ObjectStorageProxy::readObjects(common_path_prefix, blobs_to_read, read_settings, read_hint, file_size);

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
                ObjectStorageProxy::readObjects(common_path_prefix, blobs_to_read, modified_read_settings, read_hint, file_size);
            return std::make_unique<BoundedReadBuffer>(std::move(implementation_buffer));
        };

        if (blobs_to_read.size() != 1)
            throw Exception(ErrorCodes::CANNOT_USE_CACHE, "Unable to read multiple objects, support not added");

        std::string path = fs::path(common_path_prefix) / blobs_to_read[0].relative_path;
        IFileCache::Key key = getCacheKey(path);

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
    const std::string & path,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    auto modified_read_settings = getReadSettingsForCache(read_settings);
    auto impl = ObjectStorageProxy::readObject(path, read_settings, read_hint, file_size);

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
                ObjectStorageProxy::readObject(path, read_settings, read_hint, file_size);
            return std::make_unique<BoundedReadBuffer>(std::move(implementation_buffer));
        };

        IFileCache::Key key = getCacheKey(path);
        return std::make_unique<CachedReadBufferFromFile>(
            path,
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
    const std::string & path,
    WriteMode mode, // Cached doesn't support append, only rewrite
    std::optional<ObjectAttributes> attributes,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    auto impl = ObjectStorageProxy::writeObject(path, mode, attributes, std::move(finalize_callback), buf_size, write_settings);

    bool cache_on_write = fs::path(path).extension() != ".tmp"
        && write_settings.enable_filesystem_cache_on_write_operations
        && FileCacheFactory::instance().getSettings(cache->getBasePath()).cache_on_write_operations;

    if (cache_on_write)
    {
        LOG_TRACE(log, "Caching file `{}` to `{}`", path, getCachePath(path));

        auto key = getCacheKey(path);
        return std::make_unique<CachedWriteBufferFromFile>(
            std::move(impl),
            cache,
            impl->getFileName(),
            key,
            write_settings.is_file_persistent,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? CurrentThread::getQueryId().toString() : "",
            write_settings);
    }

    return impl;
}

void CachedObjectStorage::removeObject(const std::string & path)
{
    ObjectStorageProxy::removeObject(path);
}

void CachedObjectStorage::removeObjects(const std::vector<std::string> & paths)
{
    for (const auto & path : paths)
        cache->removeIfExists(getCacheKey(path));

    ObjectStorageProxy::removeObjects(paths);
}

void CachedObjectStorage::removeObjectIfExists(const std::string & path)
{
    cache->removeIfExists(getCacheKey(path));
    ObjectStorageProxy::removeObjectIfExists(path);
}

void CachedObjectStorage::removeObjectsIfExist(const std::vector<std::string> & paths)
{
    for (const auto & path : paths)
        cache->removeIfExists(getCacheKey(path));

    ObjectStorageProxy::removeObjectsIfExist(paths);
}

void CachedObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const std::string & object_from,
    const std::string & object_to,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    /// TODO: add something here?
    ObjectStorageProxy::copyObjectToAnotherObjectStorage(object_from, object_to, object_storage_to, object_to_attributes);
}

void CachedObjectStorage::copyObject( // NOLINT
    const std::string & object_from, const std::string & object_to, std::optional<ObjectAttributes> object_to_attributes)
{
    /// TODO: add something here?
    ObjectStorageProxy::copyObject(object_from, object_to, object_to_attributes);
}

std::unique_ptr<IObjectStorage> CachedObjectStorage::cloneObjectStorage(
    const std::string & new_namespace, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    /// TODO: add something here?
    return ObjectStorageProxy::cloneObjectStorage(new_namespace, config, config_prefix, context);
}

}
