#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Common/getRandomASCIIString.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_UNLINK;
}

LocalObjectStorage::LocalObjectStorage()
    : log(&Poco::Logger::get("LocalObjectStorage"))
{
    data_source_description.type = DataSourceType::Local;
    if (auto block_device_id = tryGetBlockDeviceId("/"); block_device_id.has_value())
        data_source_description.description = *block_device_id;
    else
        data_source_description.description = "/";

    data_source_description.is_cached = false;
    data_source_description.is_encrypted = false;
}

bool LocalObjectStorage::exists(const StoredObject & object) const
{
    return fs::exists(object.remote_path);
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    auto modified_settings = patchSettings(read_settings);
    auto global_context = Context::getGlobalContextInstance();
    auto read_buffer_creator =
        [=] (const std::string & file_path, size_t /* read_until_position */)
        -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return createReadBufferFromFileBase(file_path, modified_settings, read_hint, file_size);
    };

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read:
        {
            return std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator), objects, modified_settings,
                global_context->getFilesystemCacheLog(), /* use_external_buffer */false);
        }
        case RemoteFSReadMethod::threadpool:
        {
            auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator), objects, modified_settings,
                global_context->getFilesystemCacheLog(), /* use_external_buffer */true);

            auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
            return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl), reader, read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
        }
    }
}

ReadSettings LocalObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    if (!read_settings.enable_filesystem_cache)
        return IObjectStorage::patchSettings(read_settings);

    auto modified_settings{read_settings};
    /// For now we cannot allow asynchronous reader from local filesystem when CachedObjectStorage is used.
    switch (modified_settings.local_fs_method)
    {
        case LocalFSReadMethod::pread_threadpool:
        case LocalFSReadMethod::pread_fake_async:
        {
            modified_settings.local_fs_method = LocalFSReadMethod::pread;
            LOG_INFO(log, "Changing local filesystem read method to `pread`");
            break;
        }
        default:
        {
            break;
        }
    }
    return IObjectStorage::patchSettings(modified_settings);
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    const auto & path = object.remote_path;

    if (!file_size)
        file_size = tryGetSizeFromFilePath(path);

    LOG_TEST(log, "Read object: {}", path);
    return createReadBufferFromFileBase(path, patchSettings(read_settings), read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> LocalObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> /* attributes */,
    size_t buf_size,
    const WriteSettings & /* write_settings */)
{
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LocalObjectStorage doesn't support append to files");

    LOG_TEST(log, "Write object: {}", object.remote_path);
    return std::make_unique<WriteBufferFromFile>(object.remote_path, buf_size);
}

void LocalObjectStorage::removeObject(const StoredObject & object)
{
    /// For local object storage files are actually removed when "metadata" is removed.
    if (!exists(object))
        return;

    if (0 != unlink(object.remote_path.data()))
        throwFromErrnoWithPath("Cannot unlink file " + object.remote_path, object.remote_path, ErrorCodes::CANNOT_UNLINK);
}

void LocalObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObject(object);
}

void LocalObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    if (exists(object))
        removeObject(object);
}

void LocalObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata LocalObjectStorage::getObjectMetadata(const std::string & /* path */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Metadata is not supported for LocalObjectStorage");
}

void LocalObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from, const StoredObject & object_to, std::optional<ObjectAttributes> /* object_to_attributes */)
{
    auto in = readObject(object_from);
    auto out = writeObject(object_to, WriteMode::Rewrite);
    copyData(*in, *out);
    out->finalize();
}

void LocalObjectStorage::shutdown()
{
}

void LocalObjectStorage::startup()
{
}

std::unique_ptr<IObjectStorage> LocalObjectStorage::cloneObjectStorage(
    const std::string & /* new_namespace */,
    const Poco::Util::AbstractConfiguration & /* config */,
    const std::string & /* config_prefix */, ContextPtr /* context */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "cloneObjectStorage() is not implemented for LocalObjectStorage");
}

void LocalObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & /* config */, const std::string & /* config_prefix */, ContextPtr /* context */)
{
}

std::string LocalObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    constexpr size_t key_name_total_size = 32;
    return getRandomASCIIString(key_name_total_size);
}

}
