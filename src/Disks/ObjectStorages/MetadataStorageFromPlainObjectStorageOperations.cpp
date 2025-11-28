#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorageOperations.h>

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
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/SharedLockGuard.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int FILE_ALREADY_EXISTS;
extern const int INCORRECT_DATA;
extern const int FAULT_INJECTED;
extern const int LOGICAL_ERROR;
};

namespace FailPoints
{
extern const char plain_object_storage_write_fail_on_directory_create[];
extern const char plain_object_storage_write_fail_on_directory_move[];
extern const char plain_object_storage_copy_fail_on_file_move[];
extern const char plain_object_storage_copy_temp_source_file_fail_on_file_move[];
extern const char plain_object_storage_copy_temp_target_file_fail_on_file_move[];
}

namespace
{

constexpr auto PREFIX_PATH_FILE_NAME = "prefix.path";

ObjectStorageKey createMetadataObjectKey(const std::string & object_key_prefix, const std::string & metadata_key_prefix)
{
    auto prefix = std::filesystem::path(metadata_key_prefix) / object_key_prefix;
    return ObjectStorageKey::createAsRelative(prefix.string(), PREFIX_PATH_FILE_NAME);
}

std::vector<std::string> listDirectoryRecursive(const InMemoryDirectoryPathMap & path_map, const std::string & root)
{
    std::vector<std::string> subdirs = {""};
    std::queue<std::string> unlisted_nodes;
    unlisted_nodes.push(root);

    while (!unlisted_nodes.empty())
    {
        std::string next_to_list = std::move(unlisted_nodes.front());
        unlisted_nodes.pop();

        for (const auto & child : path_map.listSubdirectories(next_to_list))
        {
            subdirs.push_back((next_to_list + child).substr(root.size()));
            unlisted_nodes.push(next_to_list + child);
        }
    }

    return subdirs;
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

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::execute()
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

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::undo()
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

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::executeMoveImpl(const std::filesystem::path & from, const std::filesystem::path & to, bool validate_content)
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Moving directory '{}' to '{}'", from, to);

    for (const auto & subdir : listDirectoryRecursive(path_map, from))
    {
        auto sub_path_to = to / subdir;
        auto sub_path_from = from / subdir;

        auto write_buf = createWriteBuf(sub_path_from, sub_path_to, validate_content);

        /// parent_path() removes the trailing '/'.
        /// Let's move in memory first to be able to see this move in undo in case of error.
        path_map.moveDirectory(sub_path_from.parent_path(), sub_path_to.parent_path());

        writeString(sub_path_to.string(), *write_buf);

        fiu_do_on(FailPoints::plain_object_storage_write_fail_on_directory_move,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", sub_path_from, sub_path_to);
        });

        write_buf->finalize();

        LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Moved directory '{}' to '{}'", sub_path_from, sub_path_to);
    }
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::execute()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    constexpr bool validate_content = true;
#else
    constexpr bool validate_content = false;
#endif

    executeMoveImpl(path_from, path_to, validate_content);
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo()
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Reversing directory move from '{}' to '{}'", path_from, path_to);
    executeMoveImpl(path_to, path_from, /*validate_content=*/false);
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

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::execute( /* metadata_lock */)
{
    /// parent_path() removes the trailing '/'
    const auto base_path = path.parent_path();
    auto optional_info = path_map.getRemotePathInfoIfExists(base_path);
    if (!optional_info)
        return;
    info = *std::move(optional_info);

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Removing directory '{}'", path);

    auto metadata_object_key = createMetadataObjectKey(info.path, metadata_key_prefix);
    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key.serialize(), /*local_path*/ path, path.string().length());
    object_storage->removeObjectIfExists(metadata_object);

    if (path_map.removePathIfExists(base_path))
    {
        auto event = object_storage->getMetadataStorageMetrics().directory_removed;
        ProfileEvents::increment(event);
    }

    remove_attempted = true;
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::undo()
{
    if (!remove_attempted)
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Reversing directory removal for '{}'", path);
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

void MetadataStorageFromPlainObjectStorageWriteFileOperation::execute()
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

void MetadataStorageFromPlainObjectStorageWriteFileOperation::undo()
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

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::execute()
{
    LOG_TEST(
        getLogger("MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation"),
        "Unlinking metadata for a write '{}' with remote path '{}'",
        path,
        remote_path);

    if (path_map.removeFile(path))
        unlinked = true;
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::undo()
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

void MetadataStorageFromPlainObjectStorageCopyFileOperation::execute()
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

void MetadataStorageFromPlainObjectStorageCopyFileOperation::undo()
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


MetadataStorageFromPlainObjectStorageMoveFileOperation::MetadataStorageFromPlainObjectStorageMoveFileOperation(
    bool replaceable_,
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    InMemoryDirectoryPathMap & path_map_,
    ObjectStoragePtr object_storage_)
    : replaceable(replaceable_)
    , path_from(path_from_)
    , remote_path_from(object_storage_->generateObjectKeyForPath(path_from_, std::nullopt).serialize())
    , path_to(path_to_)
    , remote_path_to(object_storage_->generateObjectKeyForPath(path_to_, std::nullopt).serialize())
    , path_map(path_map_)
    , object_storage(object_storage_)
{
    {
        auto tmp_path_from = path_to.string() + "." + getRandomASCIIString(16) + ".tmp_move_from";
        tmp_remote_path_from = object_storage->generateObjectKeyForPath(tmp_path_from, std::nullopt).serialize();
    }
    {
        auto tmp_path_to = path_to.string() + "." + getRandomASCIIString(16) + ".tmp_move_to";
        tmp_remote_path_to = object_storage->generateObjectKeyForPath(tmp_path_to, std::nullopt).serialize();
    }
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::execute()
{
    LOG_TEST(
        getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation"),
        "Moving file (replaceable = {}) from '{}' to '{}'",
        replaceable,
        path_from,
        path_to);

    if (!path_map.existsFile(path_from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the source path '{}' does not exist", path_from);

    const auto directory_to = path_to.parent_path();
    if (!path_map.existsLocalPath(directory_to))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the target directory path '{}' does not exist", path_to);

    const auto read_settings = getReadSettingsForMetadata();
    const auto write_settings = getWriteSettingsForMetadata();

    if (path_map.existsFile(path_to))
    {
        if (!replaceable)
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

        fiu_do_on(FailPoints::plain_object_storage_copy_temp_target_file_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });

        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_to),
            /*object_to=*/StoredObject(tmp_remote_path_to),
            read_settings,
            write_settings);
        moved_existing_target_file = true;
        object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    }
    else
    {
        [[maybe_unused]] bool added = path_map.addFile(path_to);
        chassert(added);
    }

    {
        fiu_do_on(FailPoints::plain_object_storage_copy_temp_source_file_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });

        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_from),
            /*object_to=*/StoredObject(tmp_remote_path_from),
            read_settings,
            write_settings);
        moved_existing_source_file = true;
    }

    {
        fiu_do_on(FailPoints::plain_object_storage_copy_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });
        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_from), /*object_to=*/StoredObject(remote_path_to), read_settings, write_settings);
        object_storage->removeObjectIfExists(StoredObject(remote_path_from));
        moved_file = true;
    }

    path_map.removeFile(path_from);
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::undo()
{
    path_map.addFile(path_from);

    const auto read_settings = getReadSettings();
    const auto write_settings = getWriteSettings();

    if (moved_file)
    {
        LOG_WARNING(
            getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation"),
            "Removing file '{}' that was moved (replaceable = {}) from '{}",
            path_to,
            replaceable,
            path_from);

        object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    }

    if (moved_existing_source_file)
    {
        object_storage->copyObject(
            /*object_from=*/StoredObject(tmp_remote_path_from),
            /*object_to=*/StoredObject(remote_path_from),
            read_settings,
            write_settings);

        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));
    }

    if (moved_existing_target_file)
    {
        object_storage->copyObject(
            /*object_from=*/StoredObject(tmp_remote_path_to),
            /*object_to=*/StoredObject(remote_path_to),
            read_settings,
            write_settings);

        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
    }
    else
    {
        path_map.removeFile(path_to);
    }
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::finalize()
{
    if (moved_existing_source_file)
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));

    if (moved_existing_target_file)
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
}

MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
    /// path_ must end with a trailing '/'.
    std::filesystem::path && path_,
    InMemoryDirectoryPathMap & path_map_,
    ObjectStoragePtr object_storage_,
    ObjectMetadataCachePtr object_metadata_cache_,
    const std::string & metadata_key_prefix_)
    : path(std::move(path_))
    , path_map(path_map_)
    , object_storage(std::move(object_storage_))
    , object_metadata_cache(std::move(object_metadata_cache_))
    , metadata_key_prefix(metadata_key_prefix_)
    , log(getLogger("MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation"))
{
    std::string base_path = path;
    if (base_path.ends_with('/'))
        base_path.pop_back();

    path = base_path;
    tmp_path = "remove_recursive." + getRandomASCIIString(16);
    move_to_tmp_op = std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(path / "", tmp_path / "", path_map, object_storage, metadata_key_prefix);
}

void MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::execute()
{
    if (path_map.existsLocalPath(path))
    {
        move_tried = true;
        move_to_tmp_op->execute();
    }
}

void MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::undo()
{
    if (move_tried)
    {
        move_to_tmp_op->undo();
    }
}

void MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::finalize()
{
    if (!move_tried)
        return;

    StoredObjects objects_to_remove;
    for (const auto & subdir : listDirectoryRecursive(path_map, tmp_path / ""))
    {
        auto subdir_path = tmp_path / subdir;
        LOG_TRACE(log, "Removing directory '{}'", subdir_path);

        /// Info should exist since it's lifetime is bounded to execution of this operation, because tmp path is unique.
        auto directory_info = path_map.getRemotePathInfoIfExists(subdir_path.parent_path()).value();
        auto metadata_object_key = createMetadataObjectKey(directory_info.path, metadata_key_prefix);
        objects_to_remove.emplace_back(metadata_object_key.serialize(), path / PREFIX_PATH_FILE_NAME);

        /// We also need to remove all files inside each of the subdirectories.
        for (const auto & file : path_map.listFiles(subdir_path))
        {
            auto file_path = subdir_path / file;
            LOG_TRACE(log, "Removing file '{}'", file_path);

            auto file_object_key = object_storage->generateObjectKeyForPath(file_path, std::nullopt).serialize();
            objects_to_remove.emplace_back(file_object_key, file_path);

            if (object_metadata_cache)
            {
                SipHash hash;
                hash.update(file_object_key);
                object_metadata_cache->remove(hash.get128());
            }
        }

        const bool is_removed = path_map.removePathIfExists(subdir_path.parent_path());
        if (!is_removed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't remove '{}' path from in memory map. Probably it does not exist. It is a bug.", subdir_path);
    }

    object_storage->removeObjectsIfExist(objects_to_remove);
}

}
