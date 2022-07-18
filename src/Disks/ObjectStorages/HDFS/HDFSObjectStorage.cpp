#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>

#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/copyData.h>

#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/HDFS/HDFSCommon.h>

#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Common/getRandomASCIIString.h>


#if USE_HDFS

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int HDFS_ERROR;
}

void HDFSObjectStorage::shutdown()
{
}

void HDFSObjectStorage::startup()
{
}

std::string HDFSObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    return getRandomASCIIString();
}

bool HDFSObjectStorage::exists(const StoredObject & object) const
{
    const auto & path = object.absolute_path;
    const size_t begin_of_path = path.find('/', path.find("//") + 2);
    const String remote_fs_object_path = path.substr(begin_of_path);
    return (0 == hdfsExists(hdfs_fs.get(), remote_fs_object_path.c_str()));
}

std::unique_ptr<ReadBufferFromFileBase> HDFSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromHDFS>(object.absolute_path, object.absolute_path, config, read_settings);
}

std::unique_ptr<ReadBufferFromFileBase> HDFSObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto hdfs_impl = std::make_unique<ReadBufferFromHDFSGather>(config, objects, read_settings);
    auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(hdfs_impl));
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings->min_bytes_for_seek);
}

std::unique_ptr<WriteBufferFromFileBase> HDFSObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings &)
{
    if (attributes.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "HDFS API doesn't support custom attributes/metadata for stored objects");

    /// Single O_WRONLY in libhdfs adds O_TRUNC
    auto hdfs_buffer = std::make_unique<WriteBufferFromHDFS>(
        object.absolute_path, config, settings->replication, buf_size,
        mode == WriteMode::Rewrite ? O_WRONLY : O_WRONLY | O_APPEND);

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(std::move(hdfs_buffer), std::move(finalize_callback), object.absolute_path);
}


void HDFSObjectStorage::listPrefix(const std::string & path, RelativePathsWithSize & children) const
{
    const size_t begin_of_path = path.find('/', path.find("//") + 2);
    int32_t num_entries;
    auto * files_list = hdfsListDirectory(hdfs_fs.get(), path.substr(begin_of_path).c_str(), &num_entries);
    if (num_entries == -1)
        throw Exception(ErrorCodes::HDFS_ERROR, "HDFSDelete failed with path: " + path);

    for (int32_t i = 0; i < num_entries; ++i)
        children.emplace_back(files_list[i].mName, files_list[i].mSize);
}

/// Remove file. Throws exception if file doesn't exists or it's a directory.
void HDFSObjectStorage::removeObject(const StoredObject & object)
{
    const auto & path = object.absolute_path;
    const size_t begin_of_path = path.find('/', path.find("//") + 2);

    /// Add path from root to file name
    int res = hdfsDelete(hdfs_fs.get(), path.substr(begin_of_path).c_str(), 0);
    if (res == -1)
        throw Exception(ErrorCodes::HDFS_ERROR, "HDFSDelete failed with path: " + path);

}

void HDFSObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObject(object);
}

void HDFSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    if (exists(object))
        removeObject(object);
}

void HDFSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata HDFSObjectStorage::getObjectMetadata(const std::string &) const
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "HDFS API doesn't support custom attributes/metadata for stored objects");
}

void HDFSObjectStorage::copyObject( /// NOLINT
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


void HDFSObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration &, const std::string &, ContextPtr)
{
}

std::unique_ptr<IObjectStorage> HDFSObjectStorage::cloneObjectStorage(const std::string &, const Poco::Util::AbstractConfiguration &, const std::string &, ContextPtr)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS object storage doesn't support cloning");
}

}

#endif
