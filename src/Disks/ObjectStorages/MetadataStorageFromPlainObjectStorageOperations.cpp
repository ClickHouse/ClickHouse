#include "MetadataStorageFromPlainObjectStorageOperations.h"
#include <Disks/ObjectStorages/InMemoryDirectoryPathMap.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

#include <filesystem>
#include <mutex>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Timestamp.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/SharedLockGuard.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int FILE_ALREADY_EXISTS;
extern const int INCORRECT_DATA;
extern const int FAULT_INJECTED;
};

namespace FailPoints
{
extern const char plain_object_storage_write_fail_on_directory_create[];
extern const char plain_object_storage_write_fail_on_directory_move[];
}

namespace
{

constexpr auto PREFIX_PATH_FILE_NAME = "prefix.path";

ObjectStorageKey createMetadataObjectKey(const std::string & object_key_prefix, const std::string & metadata_key_prefix)
{
    auto prefix = std::filesystem::path(metadata_key_prefix) / object_key_prefix;
    return ObjectStorageKey::createAsRelative(prefix.string(), PREFIX_PATH_FILE_NAME);
}
}

MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
    std::filesystem::path && path_,
    InMemoryDirectoryPathMap & path_map_,
    ObjectStoragePtr object_storage_,
    const std::string & metadata_key_prefix_)
    : path(std::move(path_))
    , path_map(path_map_)
    , object_storage(object_storage_)
    , metadata_key_prefix(metadata_key_prefix_)
    , object_key_prefix(object_storage->generateObjectKeyPrefixForDirectoryPath(path, "" /* object_key_prefix */).serialize())
{
    chassert(path.empty() || path.string().ends_with('/'));
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::execute(std::unique_lock<SharedMutex> &)
{
    /// parent_path() removes the trailing '/'
    const auto base_path = path.parent_path();
    if (path_map.existsLocalPath(base_path))
        return;

    auto metadata_object_key = createMetadataObjectKey(object_key_prefix, metadata_key_prefix);

    LOG_TRACE(
        getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"),
        "Creating metadata for directory '{}' with remote path='{}'",
        path,
        metadata_object_key.serialize());

    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key.serialize(), /*local_path*/ path / PREFIX_PATH_FILE_NAME);

    size_t buf_size = std::bit_ceil(path.string().size()) << 1;
    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ std::clamp(buf_size, 32lu, size_t(DBMS_DEFAULT_BUFFER_SIZE)),
        /*settings*/ getWriteSettings());

    writeString(path.string(), *buf);
    fiu_do_on(FailPoints::plain_object_storage_write_fail_on_directory_create, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when creating '{}' directory", path);
    });
    buf->finalize();

    auto event = object_storage->getMetadataStorageMetrics().directory_created;
    ProfileEvents::increment(event);
    auto metadata = object_storage->getObjectMetadata(metadata_object.remote_path);
    path_map.addOrReplacePath(base_path, InMemoryDirectoryPathMap::RemotePathInfo{object_key_prefix, metadata.etag, metadata.last_modified.epochTime(), {}});
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"), "Reversing directory creation for path '{}'", path);
    const auto base_path = path.parent_path();
    path_map.removePathIfExists(base_path);
    auto metadata_object_key = createMetadataObjectKey(object_key_prefix, metadata_key_prefix);
    object_storage->removeObjectIfExists(StoredObject(metadata_object_key.serialize(), path / PREFIX_PATH_FILE_NAME));
}

MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
    std::filesystem::path && path_from_,
    std::filesystem::path && path_to_,
    InMemoryDirectoryPathMap & path_map_,
    ObjectStoragePtr object_storage_,
    const std::string & metadata_key_prefix_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , path_map(path_map_)
    , object_storage(object_storage_)
    , metadata_key_prefix(metadata_key_prefix_)
{
    chassert(path_from.empty() || path_from.string().ends_with('/'));
    chassert(path_to.empty() || path_to.string().ends_with('/'));
}

std::unique_ptr<WriteBufferFromFileBase> MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::createWriteBuf(
    const std::filesystem::path & expected_path, const std::filesystem::path & new_path, bool validate_content)
{
    /// parent_path() removes the trailing '/'.
    auto remote_path_info = path_map.getRemotePathInfoIfExists(expected_path.parent_path());
    if (!remote_path_info)
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the expected (source) path '{}' does not exist", expected_path);

    if (path_map.existsLocalPath(new_path.parent_path()))
        throw Exception(
            ErrorCodes::FILE_ALREADY_EXISTS, "Metadata object for the new (destination) path '{}' already exists", new_path);

    std::filesystem::path remote_path = remote_path_info->path;

    auto metadata_object_key = createMetadataObjectKey(remote_path, metadata_key_prefix);

    auto metadata_object
        = StoredObject(/*remote_path*/ metadata_object_key.serialize(), /*local_path*/ expected_path / PREFIX_PATH_FILE_NAME);

    if (validate_content)
    {
        LockMemoryExceptionInThread temporarily_lock_exceptions;

        std::string data;
        auto read_settings = getReadSettings();
        read_settings.remote_fs_method = RemoteFSReadMethod::threadpool;
        read_settings.remote_fs_prefetch = false;
        read_settings.remote_fs_buffer_size = 1024;

        auto read_buf = object_storage->readObject(metadata_object, read_settings);
        readStringUntilEOF(data, *read_buf);
        if (data != expected_path)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Incorrect data for object key {}, expected {}, got {}",
                metadata_object_key.serialize(),
                expected_path,
                data);
    }

    size_t buf_size = std::bit_ceil(new_path.string().size()) << 1;
    auto write_buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ std::clamp(buf_size, 32lu, size_t(DBMS_DEFAULT_BUFFER_SIZE)),
        /*settings*/ getWriteSettings());

    return write_buf;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::execute(std::unique_lock<SharedMutex> & /* metadata_lock */)
{
    LOG_TRACE(
        getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Moving directory '{}' to '{}'", path_from, path_to);

#ifdef DEBUG_OR_SANITIZER_BUILD
    constexpr bool validate_content = true;
#else
    constexpr bool validate_content = false;
#endif

    std::unordered_set<std::string> subdirs = {""};
    path_map.iterateSubdirectories(path_from.parent_path().string() + "/", [&](const auto & elem){ subdirs.emplace(elem); });
    for (const auto & subdir : subdirs)
    {
        auto sub_path_to = path_to / subdir;
        auto sub_path_from = path_from / subdir;

        auto write_buf = createWriteBuf(sub_path_from, sub_path_to, validate_content);
        writeString(sub_path_to.string(), *write_buf);

        fiu_do_on(FailPoints::plain_object_storage_write_fail_on_directory_move,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", sub_path_from, sub_path_to);
        });

        write_buf->finalize();

        /// parent_path() removes the trailing '/'.
        path_map.moveDirectory(sub_path_from.parent_path(), sub_path_to.parent_path());

        LOG_TEST(
            getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Moved directory '{}' to '{}'", sub_path_from, sub_path_to);
    }

    write_finalized = true;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (write_finalized)
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"), "Reversing directory move from '{}' to '{}'", path_from, path_to);
        path_map.moveDirectory(path_to.parent_path(), path_from.parent_path());

        auto write_buf = createWriteBuf(path_to, path_from, /* verify_content */ false);
        writeString(path_from.string(), *write_buf);
        write_buf->finalize();
    }
}

MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
    std::filesystem::path && path_,
    InMemoryDirectoryPathMap & path_map_,
    ObjectStoragePtr object_storage_,
    const std::string & metadata_key_prefix_)
    : path(std::move(path_)), path_map(path_map_), object_storage(object_storage_), metadata_key_prefix(metadata_key_prefix_)
{
    chassert(path.empty() || path.string().ends_with('/'));
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::execute(std::unique_lock<SharedMutex> & /* metadata_lock */)
{
    /// parent_path() removes the trailing '/'
    const auto base_path = path.parent_path();
    auto optional_info = path_map.getRemotePathInfoIfExists(base_path);
    if (!optional_info)
        return;
    info = *std::move(optional_info);

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Removing directory '{}'", path);

    auto metadata_object_key = createMetadataObjectKey(info.path, metadata_key_prefix);
    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key.serialize(), /*local_path*/ path / PREFIX_PATH_FILE_NAME);
    object_storage->removeObjectIfExists(metadata_object);

    if (path_map.removePathIfExists(base_path))
    {
        auto event = object_storage->getMetadataStorageMetrics().directory_removed;
        ProfileEvents::increment(event);
    }

    remove_attempted = true;
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (!remove_attempted)
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"), "Reversing directory removal for '{}'", path);
    path_map.addOrReplacePath(path.parent_path(), info);

    auto metadata_object_key = createMetadataObjectKey(info.path, metadata_key_prefix);
    auto metadata_object = StoredObject(metadata_object_key.serialize(), path / PREFIX_PATH_FILE_NAME);

    size_t buf_size = std::bit_ceil(path.string().size()) << 1;
    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ std::clamp(buf_size, 32lu, size_t(DBMS_DEFAULT_BUFFER_SIZE)),
        /*settings*/ DB::getWriteSettings());
    writeString(path.string(), *buf);
    buf->finalize();
}

MetadataStorageFromPlainObjectStorageWriteFileOperation::MetadataStorageFromPlainObjectStorageWriteFileOperation(
    const std::string & path_, InMemoryDirectoryPathMap & path_map_, ObjectStoragePtr object_storage_)
    : path(path_), path_map(path_map_), object_storage(object_storage_)
{
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::execute(std::unique_lock<SharedMutex> &)
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"), "Creating metadata for a file '{}'", path);

    if (path_map.addFile(path))
    {
        written = true;
    }
    else
    {
        /// Some paths (e.g., clickhouse_access_check) may not have parent directories.
        LOG_TRACE(
            getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"),
            "Parent directory does not exist, skipping path {}",
            path);
    }
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (!written)
        return;

    path_map.removeFile(path);
}

MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
    std::filesystem::path && path_, InMemoryDirectoryPathMap & path_map_, ObjectStoragePtr object_storage_)
    : path(path_)
    , remote_path(std::filesystem::path(object_storage_->generateObjectKeyForPath(path_, std::nullopt).serialize()))
    , path_map(path_map_)
    , object_storage(object_storage_)
{
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::execute(std::unique_lock<SharedMutex> &)
{
    LOG_TEST(
        getLogger("MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation"),
        "Unlinking metadata for a write '{}' with remote path '{}'",
        path,
        remote_path);

    if (path_map.removeFile(path))
        unlinked = true;
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (!unlinked)
        return;

    if (!path_map.addFile(path))
    {
        /// Some paths (e.g., clickhouse_access_check) may not have parent directories.
        LOG_TRACE(
            getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"),
            "Parent directory does not exist, skipping path {}",
            path);
    }
}
MetadataStorageFromPlainObjectStorageCopyFileOperation::MetadataStorageFromPlainObjectStorageCopyFileOperation(
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    InMemoryDirectoryPathMap & path_map_,
    ObjectStoragePtr object_storage_)
    : path_from(path_from_)
    , remote_path_from(object_storage_->generateObjectKeyForPath(path_from_, std::nullopt).serialize())
    , path_to(path_to_)
    , remote_path_to(object_storage_->generateObjectKeyForPath(path_to_, std::nullopt).serialize())
    , path_map(path_map_)
    , object_storage(object_storage_)
{
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::execute(std::unique_lock<SharedMutex> & /*metadata_lock*/)
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation"), "Copying file from '{}' to '{}'", path_from, path_to);

    if (!path_map.existsFile(path_from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the source path '{}' does not exist", path_from);

    const auto directory_to = path_to.parent_path();
    if (!path_map.existsLocalPath(directory_to))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the target directory path '{}' does not exist", path_to);

    if (path_map.existsFile(path_to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

    object_storage->copyObject(StoredObject(remote_path_from), StoredObject(remote_path_to), getReadSettings(), getWriteSettings());

    copied = true;
    [[maybe_unused]] bool added = path_map.addFile(path_to);
    chassert(added);
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::undo(std::unique_lock<SharedMutex> & /*metadata_lock*/)
{
    if (!copied)
        return;

    LOG_WARNING(
        getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation"),
        "Removing file '{}' that was copied from '{}",
        path_to,
        path_from);

    object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    path_map.removeFile(path_to);
}
}
