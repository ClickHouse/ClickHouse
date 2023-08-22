#include "CFSObjectStorage.h"
#include <aws/core/utils/DateTime.h>
#include <Common/getRandomASCIIString.h>
#include <Interpreters/Context.h>
#include <IO/copyData.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/CFS/ReadBufferFromCFS.h>
#include <IO/CFS/WriteBufferFromCFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_UNLINK;
    extern const int UNSUPPORTED_METHOD;
}

void CFSObjectStorage::startup()
{
    if (!fs::is_directory(data_source_description.description))
    {
        try
        {
            fs::create_directories(data_source_description.description);
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot create the directory of disk {} ({}).", name, data_source_description.description);
            throw;
        }
    }
}

void CFSObjectStorage::shutdown()
{
}

std::string CFSObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    /// Path to store the new CFS object.

    /// Total length is 32 a-z characters for enough randomness.
    /// First 3 characters are used as a prefix for
    constexpr size_t key_name_total_size = 32;
    constexpr size_t key_name_prefix_size = 3;
    const String & date = Aws::Utils::DateTime::CalculateLocalTimestampAsString("%Y%m%d");

    /// Path to store new CFS object.
    return fmt::format("{}/{}/{}",
                       date,
                       getRandomASCIIString(key_name_prefix_size),
                       getRandomASCIIString(key_name_total_size - key_name_prefix_size));
}

bool CFSObjectStorage::exists(const StoredObject & object) const
{
    return fs::exists(fs::path(object.remote_path));
}

void CFSObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const
{
    int key_count = 0;
    for (const auto & entry : fs::directory_iterator(fs::path(data_source_description.description) / path))
    {
        children.emplace_back(entry.path().filename(), ObjectMetadata{entry.file_size(), {}, {}});
        key_count++;
        if (key_count >= max_keys)
            break;
    }
    LOG_TEST(log, "List objects path {}, children size {}", path, children.size());
}

std::unique_ptr<ReadBufferFromFileBase> CFSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromCFS>(object.remote_path, patchSettings(read_settings));
}

std::unique_ptr<ReadBufferFromFileBase> CFSObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto disk_read_settings = patchSettings(read_settings);
    auto global_context = Context::getGlobalContextInstance();

    auto read_buffer_creator =
        [this, disk_read_settings]
        (const std::string & path, size_t read_until_position) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        auto buffer_size = disk_read_settings.remote_fs_buffer_size;
        return std::make_unique<ReadBufferFromCFS>(
            fs::path(path),
            disk_read_settings,
            settings->cfs_max_single_read_retries,
            /* offset */0,
            read_until_position ? read_until_position : buffer_size,
            /* use_external_buffer */true);
    };

    auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
            std::move(read_buffer_creator),
            objects,
            disk_read_settings,
            global_context->getFilesystemCacheLog(),
            /* use_external_buffer */false);

    if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
        return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl),
                reader,
                disk_read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
    }
    else
        return impl;
}

std::unique_ptr<WriteBufferFromFileBase> CFSObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    if (attributes.has_value())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "CFS API doesn't support custom attributes/metadata for stored objects");

    const String & cfs_path = object.remote_path.substr(0, object.remote_path.find_last_of('/') + 1);
    if (!fs::is_directory(cfs_path))
    {
        try
        {
            fs::create_directories(cfs_path);
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot create the directory of disk {} ({}).", name, cfs_path);
            throw;
        }
    }
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromCFS>(
            object.remote_path,
            config,
            patchSettings(write_settings),
            buf_size,
            flags);
}

/// Remove file. Throws exception if file doesn't exists or it's a directory.
void CFSObjectStorage::removeObject(const StoredObject & object)
{
    auto cfs_path = fs::path(object.remote_path);
    if (0 != unlink(cfs_path.c_str()) && errno != ENOENT)
        throwFromErrnoWithPath("Cannot unlink file " + cfs_path.string(), cfs_path, ErrorCodes::CANNOT_UNLINK);
}

void CFSObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObject(object);
}

void CFSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    if (exists(object))
        removeObject(object);
}

void CFSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata CFSObjectStorage::getObjectMetadata(const std::string &) const
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "CFS API doesn't support custom attributes/metadata for stored objects");
}

void CFSObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    if (object_to_attributes.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "CFS API doesn't support custom attributes/metadata for stored objects");

    auto in = readObject(object_from);
    auto out = writeObject(object_to, WriteMode::Rewrite);
    copyData(*in, *out);
    out->finalize();
}

std::unique_ptr<IObjectStorage> CFSObjectStorage::cloneObjectStorage(const std::string &, const Poco::Util::AbstractConfiguration &, const std::string &, ContextPtr)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "CFS object storage doesn't support cloning");
}

}
