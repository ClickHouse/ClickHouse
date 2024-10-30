#include "MetadataStorageFromPlainObjectStorageOperations.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
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

}

MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
    std::filesystem::path && path_,
    std::string && key_prefix_,
    MetadataStorageFromPlainObjectStorage::PathMap & path_map_,
    ObjectStoragePtr object_storage_)
    : path(std::move(path_)), key_prefix(key_prefix_), path_map(path_map_), object_storage(object_storage_)
{
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::execute(std::unique_lock<SharedMutex> &)
{
    if (path_map.contains(path))
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"), "Creating metadata for directory '{}'", path);

    auto object_key = ObjectStorageKey::createAsRelative(key_prefix, PREFIX_PATH_FILE_NAME);

    auto object = StoredObject(object_key.serialize(), path / PREFIX_PATH_FILE_NAME);
    auto buf = object_storage->writeObject(
        object,
        WriteMode::Rewrite,
        /* object_attributes */ std::nullopt,
        /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE,
        /* settings */ {});

    write_created = true;

    [[maybe_unused]] auto result = path_map.emplace(path, std::move(key_prefix));
    chassert(result.second);
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
    auto object_key = ObjectStorageKey::createAsRelative(key_prefix, PREFIX_PATH_FILE_NAME);
    if (write_finalized)
    {
        path_map.erase(path);
        auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
        CurrentMetrics::sub(metric, 1);

        object_storage->removeObject(StoredObject(object_key.serialize(), path / PREFIX_PATH_FILE_NAME));
    }
    else if (write_created)
        object_storage->removeObjectIfExists(StoredObject(object_key.serialize(), path / PREFIX_PATH_FILE_NAME));
}

MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
    std::filesystem::path && path_from_,
    std::filesystem::path && path_to_,
    MetadataStorageFromPlainObjectStorage::PathMap & path_map_,
    ObjectStoragePtr object_storage_)
    : path_from(std::move(path_from_)), path_to(std::move(path_to_)), path_map(path_map_), object_storage(object_storage_)
{
}

std::unique_ptr<WriteBufferFromFileBase> MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::createWriteBuf(
    const std::filesystem::path & expected_path, const std::filesystem::path & new_path, bool validate_content)
{
    auto expected_it = path_map.find(expected_path);
    if (expected_it == path_map.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the expected (source) path '{}' does not exist", expected_path);

    if (path_map.contains(new_path))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Metadata object for the new (destination) path '{}' already exists", new_path);

    auto object_key = ObjectStorageKey::createAsRelative(expected_it->second, PREFIX_PATH_FILE_NAME);

    auto object = StoredObject(object_key.serialize(), expected_path / PREFIX_PATH_FILE_NAME);

    if (validate_content)
    {
        std::string data;
        auto read_buf = object_storage->readObject(object);
        readStringUntilEOF(data, *read_buf);
        if (data != path_from)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Incorrect data for object key {}, expected {}, got {}",
                object_key.serialize(),
                expected_path,
                data);
    }

    auto write_buf = object_storage->writeObject(
        object,
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

    [[maybe_unused]] auto result = path_map.emplace(path_to, path_map.extract(path_from).mapped());
    chassert(result.second);

    write_finalized = true;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (write_finalized)
        path_map.emplace(path_from, path_map.extract(path_to).mapped());

    if (write_created)
    {
        auto write_buf = createWriteBuf(path_to, path_from, /* verify_content */ false);
        writeString(path_from.string(), *write_buf);
        write_buf->finalize();
    }
}

MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
    std::filesystem::path && path_, MetadataStorageFromPlainObjectStorage::PathMap & path_map_, ObjectStoragePtr object_storage_)
    : path(std::move(path_)), path_map(path_map_), object_storage(object_storage_)
{
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::execute(std::unique_lock<SharedMutex> & /* metadata_lock */)
{
    auto path_it = path_map.find(path);
    if (path_it == path_map.end())
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Removing directory '{}'", path);

    key_prefix = path_it->second;
    auto object_key = ObjectStorageKey::createAsRelative(key_prefix, PREFIX_PATH_FILE_NAME);
    auto object = StoredObject(object_key.serialize(), path / PREFIX_PATH_FILE_NAME);
    object_storage->removeObject(object);

    path_map.erase(path_it);
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

    auto object_key = ObjectStorageKey::createAsRelative(key_prefix, PREFIX_PATH_FILE_NAME);
    auto object = StoredObject(object_key.serialize(), path / PREFIX_PATH_FILE_NAME);
    auto buf = object_storage->writeObject(
        object,
        WriteMode::Rewrite,
        /* object_attributes */ std::nullopt,
        /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE,
        /* settings */ {});
    writeString(path.string(), *buf);
    buf->finalize();

    path_map.emplace(path, std::move(key_prefix));
    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::add(metric, 1);
}

}
