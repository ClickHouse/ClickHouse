#include "MetadataStorageFromPlainObjectStorageOperations.h"
#include <Disks/ObjectStorages/InMemoryPathMap.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/SharedLockGuard.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int FILE_ALREADY_EXISTS;
extern const int INCORRECT_DATA;
};

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
    std::filesystem::path && path_, InMemoryPathMap & path_map_, ObjectStoragePtr object_storage_, const std::string & metadata_key_prefix_)
    : path(std::move(path_))
    , path_map(path_map_)
    , object_storage(object_storage_)
    , metadata_key_prefix(metadata_key_prefix_)
    , object_key_prefix(object_storage->generateObjectKeyPrefixForDirectoryPath(path, "" /* object_key_prefix */).serialize())
{
    chassert(path.string().ends_with('/'));
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::execute(std::unique_lock<SharedMutex> &)
{
    /// parent_path() removes the trailing '/'
    const auto base_path = path.parent_path();
    {
        SharedLockGuard lock(path_map.mutex);
        if (path_map.map.contains(base_path))
            return;
    }

    auto metadata_object_key = createMetadataObjectKey(object_key_prefix, metadata_key_prefix);

    LOG_TRACE(
        getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"),
        "Creating metadata for directory '{}' with remote path='{}'",
        path,
        metadata_object_key.serialize());

    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key.serialize(), /*local_path*/ path / PREFIX_PATH_FILE_NAME);
    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /* object_attributes */ std::nullopt,
        /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE,
        /* settings */ {});

    write_created = true;

    {
        std::lock_guard lock(path_map.mutex);
        auto & map = path_map.map;
        [[maybe_unused]] auto result = map.emplace(base_path, object_key_prefix);
        chassert(result.second);
    }
    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::add(metric, 1);

    writeString(path.string(), *buf);
    buf->finalize();

    write_finalized = true;

    auto event = object_storage->getMetadataStorageMetrics().directory_created;
    ProfileEvents::increment(event);
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    auto metadata_object_key = createMetadataObjectKey(object_key_prefix, metadata_key_prefix);

    if (write_finalized)
    {
        const auto base_path = path.parent_path();
        {
            std::lock_guard lock(path_map.mutex);
            path_map.map.erase(base_path);
        }
        auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
        CurrentMetrics::sub(metric, 1);

        object_storage->removeObject(StoredObject(metadata_object_key.serialize(), path / PREFIX_PATH_FILE_NAME));
    }
    else if (write_created)
        object_storage->removeObjectIfExists(StoredObject(metadata_object_key.serialize(), path / PREFIX_PATH_FILE_NAME));
}

MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
    std::filesystem::path && path_from_,
    std::filesystem::path && path_to_,
    InMemoryPathMap & path_map_,
    ObjectStoragePtr object_storage_,
    const std::string & metadata_key_prefix_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , path_map(path_map_)
    , object_storage(object_storage_)
    , metadata_key_prefix(metadata_key_prefix_)
{
    chassert(path_from.string().ends_with('/'));
    chassert(path_to.string().ends_with('/'));
}

std::unique_ptr<WriteBufferFromFileBase> MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::createWriteBuf(
    const std::filesystem::path & expected_path, const std::filesystem::path & new_path, bool validate_content)
{
    std::filesystem::path remote_path;
    {
        SharedLockGuard lock(path_map.mutex);
        auto & map = path_map.map;
        /// parent_path() removes the trailing '/'.
        auto expected_it = map.find(expected_path.parent_path());
        if (expected_it == map.end())
            throw Exception(
                ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the expected (source) path '{}' does not exist", expected_path);

        if (map.contains(new_path.parent_path()))
            throw Exception(
                ErrorCodes::FILE_ALREADY_EXISTS, "Metadata object for the new (destination) path '{}' already exists", new_path);

        remote_path = expected_it->second;
    }

    auto metadata_object_key = createMetadataObjectKey(remote_path, metadata_key_prefix);

    auto metadata_object
        = StoredObject(/*remote_path*/ metadata_object_key.serialize(), /*local_path*/ expected_path / PREFIX_PATH_FILE_NAME);

    if (validate_content)
    {
        std::string data;
        auto read_buf = object_storage->readObject(metadata_object, ReadSettings{});
        readStringUntilEOF(data, *read_buf);
        if (data != path_from)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Incorrect data for object key {}, expected {}, got {}",
                metadata_object_key.serialize(),
                expected_path,
                data);
    }

    auto write_buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /* object_attributes */ std::nullopt,
        /*buf_size*/ DBMS_DEFAULT_BUFFER_SIZE,
        /*settings*/ {});

    return write_buf;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::execute(std::unique_lock<SharedMutex> & /* metadata_lock */)
{
    LOG_TRACE(
        getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Moving directory '{}' to '{}'", path_from, path_to);

    auto write_buf = createWriteBuf(path_from, path_to, /* validate_content */ true);
    write_created = true;
    writeString(path_to.string(), *write_buf);
    write_buf->finalize();

    /// parent_path() removes the trailing '/'.
    auto base_path_to = path_to.parent_path();
    auto base_path_from = path_from.parent_path();

    {
        std::lock_guard lock(path_map.mutex);
        auto & map = path_map.map;
        [[maybe_unused]] auto result = map.emplace(base_path_to, map.extract(base_path_from).mapped());
        chassert(result.second);
    }

    write_finalized = true;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (write_finalized)
    {
        std::lock_guard lock(path_map.mutex);
        auto & map = path_map.map;
        map.emplace(path_from.parent_path(), map.extract(path_to.parent_path()).mapped());
    }

    if (write_created)
    {
        auto write_buf = createWriteBuf(path_to, path_from, /* verify_content */ false);
        writeString(path_from.string(), *write_buf);
        write_buf->finalize();
    }
}

MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
    std::filesystem::path && path_, InMemoryPathMap & path_map_, ObjectStoragePtr object_storage_, const std::string & metadata_key_prefix_)
    : path(std::move(path_)), path_map(path_map_), object_storage(object_storage_), metadata_key_prefix(metadata_key_prefix_)
{
    chassert(path.string().ends_with('/'));
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::execute(std::unique_lock<SharedMutex> & /* metadata_lock */)
{
    /// parent_path() removes the trailing '/'
    const auto base_path = path.parent_path();
    {
        SharedLockGuard lock(path_map.mutex);
        auto & map = path_map.map;
        auto path_it = map.find(base_path);
        if (path_it == map.end())
            return;
        key_prefix = path_it->second;
    }

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Removing directory '{}'", path);

    auto metadata_object_key = createMetadataObjectKey(key_prefix, metadata_key_prefix);
    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key.serialize(), /*local_path*/ path / PREFIX_PATH_FILE_NAME);
    object_storage->removeObject(metadata_object);

    {
        std::lock_guard lock(path_map.mutex);
        auto & map = path_map.map;
        map.erase(base_path);
    }

    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::sub(metric, 1);

    removed = true;

    auto event = object_storage->getMetadataStorageMetrics().directory_removed;
    ProfileEvents::increment(event);
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (!removed)
        return;

    auto metadata_object_key = createMetadataObjectKey(key_prefix, metadata_key_prefix);
    auto metadata_object = StoredObject(metadata_object_key.serialize(), path / PREFIX_PATH_FILE_NAME);
    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /* object_attributes */ std::nullopt,
        /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE,
        /* settings */ {});
    writeString(path.string(), *buf);
    buf->finalize();

    {
        std::lock_guard lock(path_map.mutex);
        auto & map = path_map.map;
        map.emplace(path.parent_path(), std::move(key_prefix));
    }
    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::add(metric, 1);
}

}
