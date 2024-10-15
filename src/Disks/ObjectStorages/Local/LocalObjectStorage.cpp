#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>

#include <filesystem>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_UNLINK;
}

LocalObjectStorage::LocalObjectStorage(String key_prefix_)
    : key_prefix(std::move(key_prefix_))
    , log(getLogger("LocalObjectStorage"))
{
    if (auto block_device_id = tryGetBlockDeviceId("/"); block_device_id.has_value())
        description = *block_device_id;
    else
        description = "/";

    fs::create_directories(key_prefix);
}

bool LocalObjectStorage::exists(const StoredObject & object) const
{
    return fs::exists(object.remote_path);
}

ReadSettings LocalObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    auto modified_settings{read_settings};
    /// Other options might break assertions in AsynchronousBoundedReadBuffer.
    modified_settings.local_fs_method = LocalFSReadMethod::pread;
    modified_settings.direct_io_threshold = 0; /// Disable.
    return IObjectStorage::patchSettings(modified_settings);
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (!file_size)
        file_size = tryGetSizeFromFilePath(object.remote_path);

    LOG_TEST(log, "Read object: {}", object.remote_path);
    return createReadBufferFromFileBase(object.remote_path, patchSettings(read_settings), read_hint, file_size);
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

    /// Unlike real blob storage, in local fs we cannot create a file with non-existing prefix.
    /// So let's create it.
    fs::create_directories(fs::path(object.remote_path).parent_path());

    return std::make_unique<WriteBufferFromFile>(object.remote_path, buf_size);
}

void LocalObjectStorage::removeObject(const StoredObject & object)
{
    /// For local object storage files are actually removed when "metadata" is removed.
    if (!exists(object))
        return;

    if (0 != unlink(object.remote_path.data()))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, object.remote_path, "Cannot unlink file {}", object.remote_path);
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

ObjectMetadata LocalObjectStorage::getObjectMetadata(const std::string & path) const
{
    ObjectMetadata object_metadata;
    LOG_TEST(log, "Getting metadata for path: {}", path);
    object_metadata.size_bytes = fs::file_size(path);
    object_metadata.last_modified = Poco::Timestamp::fromEpochTime(
        std::chrono::duration_cast<std::chrono::seconds>(fs::last_write_time(path).time_since_epoch()).count());
    return object_metadata;
}

void LocalObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t/* max_keys */) const
{
    for (const auto & entry : fs::directory_iterator(path))
    {
        if (entry.is_directory())
        {
            listObjects(entry.path(), children, 0);
            continue;
        }

        children.emplace_back(std::make_shared<RelativePathWithMetadata>(entry.path(), getObjectMetadata(entry.path())));
    }
}

bool LocalObjectStorage::existsOrHasAnyChild(const std::string & path) const
{
    /// Unlike real object storage, existence of a prefix path can be checked by
    /// just checking existence of this prefix directly, so simple exists is enough here.
    return exists(StoredObject(path));
}

void LocalObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> /* object_to_attributes */)
{
    auto in = readObject(object_from, read_settings);
    auto out = writeObject(object_to, WriteMode::Rewrite, /* attributes= */ {}, /* buf_size= */ DBMS_DEFAULT_BUFFER_SIZE, write_settings);
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

ObjectStorageKey
LocalObjectStorage::generateObjectKeyForPath(const std::string & /* path */, const std::optional<std::string> & /* key_prefix */) const
{
    constexpr size_t key_name_total_size = 32;
    return ObjectStorageKey::createAsRelative(key_prefix, getRandomASCIIString(key_name_total_size));
}

}
