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
;
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
        /* buf_size */ 4096,
        /* settings */ {});

    write_created = true;

    path_map.emplace(path, std::move(key_prefix));

    writeString(path.string(), *buf);
    buf->finalize();

    write_finalized = true;
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::undo()
{
    if (write_finalized)
        path_map.erase(path);

    if (write_created)
    {
        auto object_key = ObjectStorageKey::createAsRelative(key_prefix, PREFIX_PATH_FILE_NAME);
        object_storage->removeObject(StoredObject(object_key.serialize(), path / PREFIX_PATH_FILE_NAME));
    }
}

MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
    std::filesystem::path && path_from_,
    std::filesystem::path && path_to_,
    MetadataStorageFromPlainObjectStorage::PathMap & path_map_,
    ObjectStoragePtr object_storage_)
    : path_from(std::move(path_from_)), path_to(std::move(path_to_)), path_map(path_map_), object_storage(object_storage_)
{
}

std::unique_ptr<WriteBufferFromFileBase> MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::moveObject(
    const std::filesystem::path & from, const std::filesystem::path & to, bool validate_content)
{
    auto from_it = path_map.find(from);
    if (from_it == path_map.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the source path '{}' does not exist", from);

    if (path_map.contains(to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Metadata object for the destination path '{}' already exists", to);

    auto object_key = ObjectStorageKey::createAsRelative(from_it->second, PREFIX_PATH_FILE_NAME);

    auto object = StoredObject(object_key.serialize(), path_from / PREFIX_PATH_FILE_NAME);

    if (validate_content)
    {
        std::string data;
        auto readBuf = object_storage->readObject(object, {});
        readStringUntilEOF(data, *readBuf);
        if (data != path_from)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Incorrect data for object key {}, expected {}, got {}",
                object_key.serialize(),
                path_from,
                data);
    }

    auto write_buf = object_storage->writeObject(
        object,
        WriteMode::Rewrite,
        std::nullopt,
        /*buf_size*/ 4096,
        /*settings*/ {});

    return write_buf;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::execute(std::unique_lock<SharedMutex> & /* metadata_lock */)
{
    LOG_TRACE(
        getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Moving directory '{}' to '{}'", path_from, path_to);

    auto write_buf = moveObject(path_from, path_to, /* validate_content */ true);
    write_created = true;
    writeString(path_to.string(), *write_buf);
    write_buf->finalize();

    path_map.emplace(path_to, path_map.extract(path_from).mapped());
    write_finalized = true;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo()
{
    if (write_finalized)
        path_map.emplace(path_from, path_map.extract(path_to).mapped());

    if (write_created)
    {
        auto write_buf = moveObject(path_to, path_from, /* verify_content */ false);
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
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::undo()
{
    if (!removed)
        return;

    auto object_key = ObjectStorageKey::createAsRelative(key_prefix, PREFIX_PATH_FILE_NAME);
    auto object = StoredObject(object_key.serialize(), path / PREFIX_PATH_FILE_NAME);
    auto buf = object_storage->writeObject(
        object,
        WriteMode::Rewrite,
        /* object_attributes */ std::nullopt,
        /* buf_size */ 4096,
        /* settings */ {});
    writeString(path.string(), *buf);
    buf->finalize();

    path_map.emplace(path, std::move(key_prefix));
}

}
