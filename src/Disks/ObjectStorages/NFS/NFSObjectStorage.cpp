#include "NFSObjectStorage.h"
#include <Common/getRandomASCIIString.h>

#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>

#include <IO/copyData.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/NFS/ReadBufferFromNFS.h>
#include <IO/NFS/WriteBufferFromNFS.h>

#include <Interpreters/Context.h>


namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int CANNOT_UNLINK;
    extern const int UNSUPPORTED_METHOD;
}

void NFSObjectStorage::startup()
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

void NFSObjectStorage::shutdown()
{
}

std::string NFSObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    /// Path to store the new NFS object.

    /// Total length is 32 a-z characters for enough randomness.
    /// First 3 characters are used as a prefix for
    /// https://aws.amazon.com/premiumsupport/knowledge-center/s3-object-key-naming-pattern/

    constexpr size_t key_name_total_size = 32;
    constexpr size_t key_name_prefix_size = 3;

    /// Path to store new NFS object.
    return fmt::format("{}/{}",
                       getRandomASCIIString(key_name_prefix_size),
                       getRandomASCIIString(key_name_total_size - key_name_prefix_size));
}

bool NFSObjectStorage::exists(const StoredObject & object) const
{
    return fs::exists(fs::path(object.absolute_path));
}

void NFSObjectStorage::listPrefix(const std::string & path, RelativePathsWithSize & children) const
{
    for (const auto & entry : fs::directory_iterator(fs::path(data_source_description.description) / path))
    {
        children.emplace_back(entry.path().filename(), fs::file_size(entry.path()));
    }
    LOG_TEST(log, "list pre fix path {}, children size {}", path, children.size());
}

std::unique_ptr<ReadBufferFromFileBase> NFSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromNFS>(object.absolute_path, patchSettings(read_settings));
}

std::unique_ptr<ReadBufferFromFileBase> NFSObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto disk_read_settings = patchSettings(read_settings);
    auto read_buffer_creator =
        [this, disk_read_settings]
        (const std::string & path, size_t read_until_position) -> std::shared_ptr<ReadBufferFromFileBase>
    {
        auto buffer_size = disk_read_settings.remote_fs_buffer_size;
        return std::make_shared<ReadBufferFromNFS>(
            fs::path(path),
            disk_read_settings,
            settings->nfs_max_single_read_retries,
            /* offset */0,
            read_until_position ? read_until_position : buffer_size,
            /* use_external_buffer */true);
    };

    auto nfs_impl = std::make_unique<ReadBufferFromRemoteFSGather>(std::move(read_buffer_creator), objects, disk_read_settings);
    if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto reader = getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, disk_read_settings, std::move(nfs_impl));
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(nfs_impl), read_settings);
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings->min_bytes_for_seek);
    }
}

std::unique_ptr<WriteBufferFromFileBase> NFSObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    if (attributes.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "HDFS API doesn't support custom attributes/metadata for stored objects");

    const String & nfs_file = object.absolute_path.substr(object.absolute_path.find_last_of('/') + 1, object.absolute_path.size() - object.absolute_path.find_last_of('/'));
    const String & nfs_path = object.absolute_path.substr(0, object.absolute_path.find_last_of('/') + 1);
    if (!fs::is_directory(nfs_path))
    {
        try
        {
            fs::create_directories(nfs_path);
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot create the directory of disk {} ({}).", name, nfs_path);
            throw;
        }
    }
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    auto nfs_buffer = std::make_unique<WriteBufferFromNFS>(
        object.absolute_path, config, patchSettings(write_settings), buf_size,
        flags);

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(std::move(nfs_buffer), std::move(finalize_callback), object.absolute_path);
}

/// Remove file. Throws exception if file doesn't exists or it's a directory.
void NFSObjectStorage::removeObject(const StoredObject & object)
{
    auto nfs_path = fs::path(object.absolute_path);
    if (0 != unlink(nfs_path.c_str()) && errno != ENOENT)
        throwFromErrnoWithPath("Cannot unlink file " + nfs_path.string(), nfs_path, ErrorCodes::CANNOT_UNLINK);
}

void NFSObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObject(object);
}

void NFSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    if (exists(object))
        removeObject(object);
}

void NFSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata NFSObjectStorage::getObjectMetadata(const std::string &) const
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "HDFS API doesn't support custom attributes/metadata for stored objects");
}

void NFSObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    if (object_to_attributes.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "HDFS API doesn't support custom attributes/metadata for stored objects");

    auto in = readObject(object_from);
    auto out = writeObject(object_to, WriteMode::Rewrite);
    copyData(*in, *out);
    out->finalize();
}

void NFSObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration &, const std::string &, ContextPtr context_)
{
    applyRemoteThrottlingSettings(context_);
}

std::unique_ptr<IObjectStorage> NFSObjectStorage::cloneObjectStorage(const std::string &, const Poco::Util::AbstractConfiguration &, const std::string &, ContextPtr)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS object storage doesn't support cloning");
}

}

