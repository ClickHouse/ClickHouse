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

bool HDFSObjectStorage::exists(const std::string & hdfs_uri) const
{
    const size_t begin_of_path = hdfs_uri.find('/', hdfs_uri.find("//") + 2);
    const String remote_fs_object_path = hdfs_uri.substr(begin_of_path);
    return (0 == hdfsExists(hdfs_fs.get(), remote_fs_object_path.c_str()));
}

std::unique_ptr<SeekableReadBuffer> HDFSObjectStorage::readObject( /// NOLINT
    const std::string & path,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromHDFS>(path, path, config, read_settings.remote_fs_buffer_size);
}

std::unique_ptr<ReadBufferFromFileBase> HDFSObjectStorage::readObjects( /// NOLINT
    const PathsWithSize & paths_to_read,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto hdfs_impl = std::make_unique<ReadBufferFromHDFSGather>(config, paths_to_read, read_settings);
    auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(hdfs_impl));
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings->min_bytes_for_seek);
}

std::unique_ptr<WriteBufferFromFileBase> HDFSObjectStorage::writeObject( /// NOLINT
    const std::string & path,
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
        path, config, settings->replication, buf_size,
        mode == WriteMode::Rewrite ? O_WRONLY : O_WRONLY | O_APPEND);

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(std::move(hdfs_buffer), std::move(finalize_callback), path);
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
void HDFSObjectStorage::removeObject(const std::string & path)
{
    const size_t begin_of_path = path.find('/', path.find("//") + 2);

    /// Add path from root to file name
    int res = hdfsDelete(hdfs_fs.get(), path.substr(begin_of_path).c_str(), 0);
    if (res == -1)
        throw Exception(ErrorCodes::HDFS_ERROR, "HDFSDelete failed with path: " + path);

}

void HDFSObjectStorage::removeObjects(const PathsWithSize & paths)
{
    for (const auto & [path, _] : paths)
        removeObject(path);
}

void HDFSObjectStorage::removeObjectIfExists(const std::string & path)
{
    if (exists(path))
        removeObject(path);
}

void HDFSObjectStorage::removeObjectsIfExist(const PathsWithSize & paths)
{
    for (const auto & [path, _] : paths)
        removeObjectIfExists(path);
}

ObjectMetadata HDFSObjectStorage::getObjectMetadata(const std::string &) const
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "HDFS API doesn't support custom attributes/metadata for stored objects");
}

void HDFSObjectStorage::copyObject( /// NOLINT
    const std::string & object_from,
    const std::string & object_to,
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
