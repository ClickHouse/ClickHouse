#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>

#include <IO/copyData.h>
#include <Storages/ObjectStorage/HDFS/WriteBufferFromHDFS.h>
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>


#if USE_HDFS

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int HDFS_ERROR;
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
}

void HDFSObjectStorage::initializeHDFSFS() const
{
    if (initialized)
        return;

    std::lock_guard lock(init_mutex);
    if (initialized)
        return;

    hdfs_builder = createHDFSBuilder(url, config);
    hdfs_fs = createHDFSFS(hdfs_builder.get());
    initialized = true;
}

std::string HDFSObjectStorage::extractObjectKeyFromURL(const StoredObject & object) const
{
    /// This is very unfortunate, but for disk HDFS we made a mistake
    /// and now its behaviour is inconsistent with S3 and Azure disks.
    /// The mistake is that for HDFS we write into metadata files whole URL + data directory + key,
    /// while for S3 and Azure we write there only data_directory + key.
    /// This leads us into ambiguity that for StorageHDFS we have just key in object.remote_path,
    /// but for DiskHDFS we have there URL as well.
    auto path = object.remote_path;
    if (path.starts_with(url))
        path = path.substr(url.size());
    if (path.starts_with("/"))
        path.substr(1);
    return path;
}

ObjectStorageKey
HDFSObjectStorage::generateObjectKeyForPath(const std::string & /* path */, const std::optional<std::string> & /* key_prefix */) const
{
    initializeHDFSFS();
    /// what ever data_source_description.description value is, consider that key as relative key
    chassert(data_directory.starts_with("/"));
    return ObjectStorageKey::createAsRelative(
        fs::path(url_without_path) / data_directory.substr(1), getRandomASCIIString(32));
}

bool HDFSObjectStorage::exists(const StoredObject & object) const
{
    initializeHDFSFS();
    std::string path = object.remote_path;
    if (path.starts_with(url_without_path))
        path = path.substr(url_without_path.size());

    return (0 == hdfsExists(hdfs_fs.get(), path.c_str()));
}

std::unique_ptr<ReadBufferFromFileBase> HDFSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    initializeHDFSFS();
    auto path = extractObjectKeyFromURL(object);
    return std::make_unique<ReadBufferFromHDFS>(
        fs::path(url_without_path) / "",
        fs::path(data_directory) / path,
        config,
        patchSettings(read_settings),
        /* read_until_position */0,
        read_settings.remote_read_buffer_use_external_buffer);
}

std::unique_ptr<WriteBufferFromFileBase> HDFSObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    initializeHDFSFS();
    if (attributes.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "HDFS API doesn't support custom attributes/metadata for stored objects");

    std::string path = object.remote_path;
    if (path.starts_with("/"))
        path = path.substr(1);
    if (!path.starts_with(url))
        path = fs::path(url) / path;

    /// Single O_WRONLY in libhdfs adds O_TRUNC
    return std::make_unique<WriteBufferFromHDFS>(
        path, config, settings->replication, patchSettings(write_settings), buf_size,
        mode == WriteMode::Rewrite ? O_WRONLY : O_WRONLY | O_APPEND);
}


/// Remove file. Throws exception if file doesn't exists or it's a directory.
void HDFSObjectStorage::removeObject(const StoredObject & object)
{
    initializeHDFSFS();
    auto path = object.remote_path;
    if (path.starts_with(url_without_path))
        path = path.substr(url_without_path.size());

    /// Add path from root to file name
    int res = hdfsDelete(hdfs_fs.get(), path.c_str(), 0);
    if (res == -1)
        throw Exception(ErrorCodes::HDFS_ERROR, "HDFSDelete failed with path: {}", path);

}

void HDFSObjectStorage::removeObjects(const StoredObjects & objects)
{
    initializeHDFSFS();
    for (const auto & object : objects)
        removeObject(object);
}

void HDFSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    initializeHDFSFS();
    if (exists(object))
        removeObject(object);
}

void HDFSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    initializeHDFSFS();
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata HDFSObjectStorage::getObjectMetadata(const std::string & path) const
{
    initializeHDFSFS();
    auto * file_info = hdfsGetPathInfo(hdfs_fs.get(), path.data());
    if (!file_info)
        throw Exception(ErrorCodes::HDFS_ERROR,
                        "Cannot get file info for: {}. Error: {}", path, hdfsGetLastError());

    ObjectMetadata metadata;
    metadata.size_bytes = static_cast<size_t>(file_info->mSize);
    metadata.last_modified = Poco::Timestamp::fromEpochTime(file_info->mLastMod);

    hdfsFreeFileInfo(file_info, 1);
    return metadata;
}

void HDFSObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    initializeHDFSFS();
    LOG_TEST(log, "Trying to list files for {}", path);

    HDFSFileInfo ls;
    ls.file_info = hdfsListDirectory(hdfs_fs.get(), path.data(), &ls.length);

    if (ls.file_info == nullptr && errno != ENOENT) // NOLINT
    {
        // ignore file not found exception, keep throw other exception,
        // libhdfs3 doesn't have function to get exception type, so use errno.
        throw Exception(ErrorCodes::ACCESS_DENIED, "Cannot list directory {}: {}",
                        path, String(hdfsGetLastError()));
    }

    if (!ls.file_info && ls.length > 0)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "file_info shouldn't be null");
    }

    LOG_TEST(log, "Listed {} files for {}", ls.length, path);

    for (int i = 0; i < ls.length; ++i)
    {
        const String file_path = fs::path(ls.file_info[i].mName).lexically_normal();
        const bool is_directory = ls.file_info[i].mKind == 'D';
        if (is_directory)
        {
            listObjects(fs::path(file_path) / "", children, max_keys);
        }
        else
        {
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(
                String(file_path),
                ObjectMetadata{
                    static_cast<uint64_t>(ls.file_info[i].mSize),
                    Poco::Timestamp::fromEpochTime(ls.file_info[i].mLastMod),
                    "",
                    {}}));
        }

        if (max_keys && children.size() >= max_keys)
            break;
    }
}

void HDFSObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes)
{
    initializeHDFSFS();
    if (object_to_attributes.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "HDFS API doesn't support custom attributes/metadata for stored objects");

    auto in = readObject(object_from, read_settings);
    auto out = writeObject(object_to, WriteMode::Rewrite, /* attributes= */ {}, /* buf_size= */ DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
}


std::unique_ptr<IObjectStorage> HDFSObjectStorage::cloneObjectStorage(
    const std::string &,
    const Poco::Util::AbstractConfiguration &,
    const std::string &, ContextPtr)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS object storage doesn't support cloning");
}

}

#endif
