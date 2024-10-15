#include "EncryptedObjectStorage.h"

#if USE_SSL

#    include <filesystem>
#    include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#    include <IO/BoundedReadBuffer.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/WriteBufferFromEncryptedFile.h>
#    include <Interpreters/Context.h>
#    include <Common/CurrentThread.h>
#    include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DATA_ENCRYPTION_ERROR;
}

String EncryptedObjectStorageSettings::findKeyByFingerprint(UInt128 key_fingerprint, const String & path_for_logs) const
{
    auto it = all_keys.find(key_fingerprint);
    if (it == all_keys.end())
    {
        throw Exception(
            ErrorCodes::DATA_ENCRYPTION_ERROR, "Not found an encryption key required to decipher file {}", quoteString(path_for_logs));
    }
    return it->second;
}

EncryptedObjectStorage::EncryptedObjectStorage(
    ObjectStoragePtr object_storage_, EncryptedObjectStorageSettingsPtr enc_settings_, const std::string & enc_config_name_)
    : object_storage(object_storage_), enc_settings(enc_settings_), enc_config_name(enc_config_name_), log(&Poco::Logger::get(getName()))
{
}

ReadSettings EncryptedObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    ReadSettings modified_settings{read_settings};
    modified_settings.encryption_settings = enc_settings;
    return object_storage->patchSettings(modified_settings);
}

void EncryptedObjectStorage::startup()
{
    object_storage->startup();
}

bool EncryptedObjectStorage::exists(const StoredObject & object) const
{
    return object_storage->exists(object);
}

std::unique_ptr<ReadBufferFromFileBase> EncryptedObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    /// TODO
    return object_storage->readObject(object, patchSettings(read_settings), read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> EncryptedObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode, // EncryptedObjectStorage doesn't support append, only rewrite
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    auto modified_write_settings = IObjectStorage::patchSettings(write_settings);
    auto implementation_buffer = object_storage->writeObject(object, mode, attributes, buf_size, modified_write_settings);
    FileEncryption::Header header;
    header.algorithm = enc_settings->current_algorithm;
    header.key_fingerprint = enc_settings->current_key_fingerprint;
    header.init_vector = FileEncryption::InitVector::random();
    removeCacheIfExists(object.remote_path);
    if (enc_settings->cache_header_on_write && enc_settings->header_cache
        && modified_write_settings.enable_filesystem_cache_on_write_operations && fs::path(object.remote_path).extension() != ".tmp")
    {
        auto cache_key = FileCacheKey::fromPath(object.remote_path);
        auto out = std::make_unique<WriteBufferFromOwnString>();
        CachedOnDiskWriteBufferFromFile cache(
            std::move(out),
            enc_settings->header_cache,
            implementation_buffer->getFileName(),
            cache_key,
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? std::string(CurrentThread::getQueryId()) : "",
            modified_write_settings,
            {},
            Context::getGlobalContextInstance()->getFilesystemCacheLog());
        header.write(cache);
        cache.finalize();
    }
    return std::make_unique<WriteBufferFromEncryptedFile>(buf_size, std::move(implementation_buffer), enc_settings->current_key, header, 0);
}

void EncryptedObjectStorage::removeObject(const StoredObject & object)
{
    removeCacheIfExists(object.remote_path);
    object_storage->removeObject(object);
}

void EncryptedObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeCacheIfExists(object.remote_path);
    object_storage->removeObjects(objects);
}

void EncryptedObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeCacheIfExists(object.remote_path);
    object_storage->removeObjectIfExists(object);
}

void EncryptedObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeCacheIfExists(object.remote_path);
    object_storage->removeObjectsIfExist(objects);
}

void EncryptedObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes)
{
    object_storage->copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
}

std::unique_ptr<IObjectStorage> EncryptedObjectStorage::cloneObjectStorage(
    const std::string & new_namespace,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context)
{
    return object_storage->cloneObjectStorage(new_namespace, config, config_prefix, context);
}

void EncryptedObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    object_storage->listObjects(path, children, max_keys);
}

ObjectMetadata EncryptedObjectStorage::getObjectMetadata(const std::string & path) const
{
    return object_storage->getObjectMetadata(path);
}

void EncryptedObjectStorage::shutdown()
{
    object_storage->shutdown();
}

void EncryptedObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context,
    const ApplyNewSettingsOptions & options)
{
    object_storage->applyNewSettings(config, config_prefix, context, options);
}

String EncryptedObjectStorage::getObjectsNamespace() const
{
    return object_storage->getObjectsNamespace();
}

void EncryptedObjectStorage::removeCacheIfExists(const std::string & path)
{
    if (enc_settings->header_cache)
    {
        auto cache_key = FileCacheKey::fromPath(path);
        enc_settings->header_cache->removeKeyIfExists(cache_key, FileCache::getCommonUser().user_id);
    }
}

ObjectStorageKey
EncryptedObjectStorage::generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const
{
    return object_storage->generateObjectKeyForPath(path, key_prefix);
}
}

#endif
