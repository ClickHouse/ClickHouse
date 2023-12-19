#include "DiskObjectStorageTransactionOperation.h"
#include "Common/checkStackSize.h"

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT;
extern const int ATTEMPT_TO_READ_AFTER_EOF;
extern const int CANNOT_READ_ALL_DATA;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

CopyFileObjectStorageOperation::CopyFileObjectStorageOperation(
    IObjectStorage & object_storage_,
    IMetadataStorage & metadata_storage_,
    IObjectStorage & destination_object_storage_,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const std::string & from_path_,
    const std::string & to_path_)
    : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
    , read_settings(read_settings_)
    , write_settings(write_settings_)
    , from_path(from_path_)
    , to_path(to_path_)
    , destination_object_storage(destination_object_storage_)
{
}

String CopyFileObjectStorageOperation::getInfoForLog() const
{
    return fmt::format("CopyFileObjectStorageOperation (path_from: {}, path_to: {})", from_path, to_path);
}

void CopyFileObjectStorageOperation::execute(MetadataTransactionPtr tx)
{
    tx->createEmptyMetadataFile(to_path);
    auto source_blobs = metadata_storage.getStorageObjects(from_path); /// Full paths

    for (const auto & object_from : source_blobs)
    {
        auto object_key = object_storage.generateObjectKeyForPath(to_path);
        auto object_to = StoredObject(object_key.serialize());

        object_storage.copyObjectToAnotherObjectStorage(object_from, object_to,read_settings,write_settings, destination_object_storage);

        tx->addBlobToMetadata(to_path, object_key, object_from.bytes_size);

        created_objects.push_back(object_to);
    }
}

void CopyFileObjectStorageOperation::undo()
{
    for (const auto & object : created_objects)
        destination_object_storage.removeObject(object);
}

RemoveRecursiveObjectStorageOperation::RemoveRecursiveObjectStorageOperation(
    IObjectStorage & object_storage_,
    IMetadataStorage & metadata_storage_,
    const std::string & path_,
    bool keep_all_batch_data_,
    const NameSet & file_names_remove_metadata_only_)
    : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
    , path(path_)
    , keep_all_batch_data(keep_all_batch_data_)
    , file_names_remove_metadata_only(file_names_remove_metadata_only_)
{
}

String RemoveRecursiveObjectStorageOperation::getInfoForLog() const
{
    return fmt::format("RemoveRecursiveObjectStorageOperation (path: {})", path);
}

void RemoveRecursiveObjectStorageOperation::removeMetadataRecursive(MetadataTransactionPtr tx, const std::string & path_to_remove)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    if (metadata_storage.isFile(path_to_remove))
    {
        try
        {
            chassert(path_to_remove.starts_with(path));
            auto rel_path = String(fs::relative(fs::path(path_to_remove), fs::path(path)));

            auto objects_paths = metadata_storage.getStorageObjects(path_to_remove);
            auto unlink_outcome = tx->unlinkMetadata(path_to_remove);

            if (unlink_outcome && !file_names_remove_metadata_only.contains(rel_path))
                objects_to_remove_by_path[std::move(rel_path)] = ObjectsToRemove{std::move(objects_paths), std::move(unlink_outcome)};
        }
        catch (const Exception & e)
        {
            /// If it's impossible to read meta - just remove it from FS.
            if (e.code() == ErrorCodes::UNKNOWN_FORMAT || e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
                || e.code() == ErrorCodes::CANNOT_READ_ALL_DATA || e.code() == ErrorCodes::CANNOT_OPEN_FILE
                || e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
            {
                LOG_DEBUG(
                    &Poco::Logger::get("RemoveRecursiveObjectStorageOperation"),
                    "Can't read metadata because of an exception. Just remove it from the filesystem. Path: {}, exception: {}",
                    metadata_storage.getPath() + path_to_remove,
                    e.message());

                tx->unlinkFile(path_to_remove);
            }
            else
                throw;
        }
    }
    else
    {
        for (auto it = metadata_storage.iterateDirectory(path_to_remove); it->isValid(); it->next())
            removeMetadataRecursive(tx, it->path());

        tx->removeDirectory(path_to_remove);
    }
}

void RemoveRecursiveObjectStorageOperation::execute(MetadataTransactionPtr tx)
{
    /// Similar to DiskLocal and https://en.cppreference.com/w/cpp/filesystem/remove
    if (metadata_storage.exists(path))
        removeMetadataRecursive(tx, path);
}

void RemoveRecursiveObjectStorageOperation::finalize()
{
    if (keep_all_batch_data)
        return;

    std::vector<String> total_removed_paths;
    total_removed_paths.reserve(objects_to_remove_by_path.size());

    StoredObjects remove_from_remote;
    for (auto && [local_path, objects_to_remove] : objects_to_remove_by_path)
    {
        chassert(!file_names_remove_metadata_only.contains(local_path));
        if (objects_to_remove.unlink_outcome->num_hardlinks == 0)
        {
            std::move(objects_to_remove.objects.begin(), objects_to_remove.objects.end(), std::back_inserter(remove_from_remote));
            total_removed_paths.push_back(local_path);
        }
    }

    /// Read comment inside RemoveObjectStorageOperation class
    /// TL;DR Don't pay any attention to 404 status code
    object_storage.removeObjectsIfExist(remove_from_remote);

    LOG_DEBUG(
        &Poco::Logger::get("RemoveRecursiveObjectStorageOperation"),
        "Recursively remove path {}: "
        "metadata and objects were removed for [{}], "
        "only metadata were removed for [{}].",
        path,
        fmt::join(total_removed_paths, ", "),
        fmt::join(file_names_remove_metadata_only, ", "));
}
}
