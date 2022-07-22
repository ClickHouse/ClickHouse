#include <Disks/ObjectStorages/DiskObjectStorageRemoteMetadataRestoreHelper.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static String revisionToString(UInt64 revision)
{
    return std::bitset<64>(revision).to_string();
}

void DiskObjectStorageRemoteMetadataRestoreHelper::createFileOperationObject(
    const String & operation_name, UInt64 revision, const ObjectAttributes & metadata) const
{
    const String relative_path = "operations/r" + revisionToString(revision) + operation_log_suffix + "-" + operation_name;
    StoredObject object(fs::path(disk->object_storage_root_path) / relative_path);
    auto buf = disk->object_storage->writeObject(object, WriteMode::Rewrite, metadata);
    buf->write('0');
    buf->finalize();
}

void DiskObjectStorageRemoteMetadataRestoreHelper::findLastRevision()
{
    /// Construct revision number from high to low bits.
    String revision;
    revision.reserve(64);
    for (int bit = 0; bit < 64; ++bit)
    {
        auto revision_prefix = revision + "1";

        LOG_TRACE(disk->log, "Check object exists with revision prefix {}", revision_prefix);

        const auto & object_storage = disk->object_storage;
        StoredObject revision_object{disk->object_storage_root_path + "r" + revision_prefix};
        StoredObject revision_operation_object{disk->object_storage_root_path + "operations/r" + revision_prefix};

        /// Check file or operation with such revision prefix exists.
        if (object_storage->exists(revision_object) || object_storage->exists(revision_operation_object))
            revision += "1";
        else
            revision += "0";
    }
    revision_counter = static_cast<UInt64>(std::bitset<64>(revision).to_ullong());
    LOG_INFO(disk->log, "Found last revision number {} for disk {}", revision_counter, disk->name);
}

int DiskObjectStorageRemoteMetadataRestoreHelper::readSchemaVersion(IObjectStorage * object_storage, const String & source_path)
{
    StoredObject object(fs::path(source_path) / SCHEMA_VERSION_OBJECT);
    int version = 0;
    if (!object_storage->exists(object))
        return version;

    auto buf = object_storage->readObject(object);
    readIntText(version, *buf);

    return version;
}

void DiskObjectStorageRemoteMetadataRestoreHelper::saveSchemaVersion(const int & version) const
{
    StoredObject object{fs::path(disk->object_storage_root_path) / SCHEMA_VERSION_OBJECT};

    auto buf = disk->object_storage->writeObject(object, WriteMode::Rewrite);
    writeIntText(version, *buf);
    buf->finalize();

}

void DiskObjectStorageRemoteMetadataRestoreHelper::updateObjectMetadata(const String & key, const ObjectAttributes & metadata) const
{
    StoredObject object{key};
    disk->object_storage->copyObject(object, object, metadata);
}

void DiskObjectStorageRemoteMetadataRestoreHelper::migrateFileToRestorableSchema(const String & path) const
{
    LOG_TRACE(disk->log, "Migrate file {} to restorable schema", disk->metadata_storage->getPath() + path);

    auto objects = disk->metadata_storage->getStorageObjects(path);
    for (const auto & object : objects)
    {
        ObjectAttributes metadata {
            {"path", path}
        };
        updateObjectMetadata(object.absolute_path, metadata);
    }
}
void DiskObjectStorageRemoteMetadataRestoreHelper::migrateToRestorableSchemaRecursive(const String & path, Futures & results)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    LOG_TRACE(disk->log, "Migrate directory {} to restorable schema", disk->metadata_storage->getPath() + path);

    bool dir_contains_only_files = true;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        if (disk->isDirectory(it->path()))
        {
            dir_contains_only_files = false;
            break;
        }
    }

    /// The whole directory can be migrated asynchronously.
    if (dir_contains_only_files)
    {
        auto result = disk->getExecutor().execute([this, path]
        {
            for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
                migrateFileToRestorableSchema(it->path());
        });

        results.push_back(std::move(result));
    }
    else
    {
        for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
            if (!disk->isDirectory(it->path()))
            {
                auto source_path = it->path();
                auto result = disk->getExecutor().execute([this, source_path]
                    {
                        migrateFileToRestorableSchema(source_path);
                    });

                results.push_back(std::move(result));
            }
            else
                migrateToRestorableSchemaRecursive(it->path(), results);
    }

}

void DiskObjectStorageRemoteMetadataRestoreHelper::migrateToRestorableSchema()
{
    try
    {
        LOG_INFO(disk->log, "Start migration to restorable schema for disk {}", disk->name);

        Futures results;

        for (const auto & root : data_roots)
            if (disk->exists(root))
                migrateToRestorableSchemaRecursive(root + '/', results);

        for (auto & result : results)
            result.wait();
        for (auto & result : results)
            result.get();

        saveSchemaVersion(RESTORABLE_SCHEMA_VERSION);
    }
    catch (const Exception &)
    {
        tryLogCurrentException(disk->log, fmt::format("Failed to migrate to restorable schema for disk {}", disk->name));

        throw;
    }
}

void DiskObjectStorageRemoteMetadataRestoreHelper::restore(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    LOG_INFO(disk->log, "Restore operation for disk {} called", disk->name);

    if (!disk->exists(RESTORE_FILE_NAME))
    {
        LOG_INFO(disk->log, "No restore file '{}' exists, finishing restore", RESTORE_FILE_NAME);
        return;
    }

    try
    {
        RestoreInformation information;
        information.source_path = disk->object_storage_root_path;
        information.source_namespace = disk->object_storage->getObjectsNamespace();

        readRestoreInformation(information);
        if (information.revision == 0)
            information.revision = LATEST_REVISION;
        if (!information.source_path.ends_with('/'))
            information.source_path += '/';

        IObjectStorage * source_object_storage = disk->object_storage.get();
        if (information.source_namespace == disk->object_storage->getObjectsNamespace())
        {
            /// In this case we need to additionally cleanup S3 from objects with later revision.
            /// Will be simply just restore to different path.
            if (information.source_path == disk->object_storage_root_path && information.revision != LATEST_REVISION)
                throw Exception("Restoring to the same bucket and path is allowed if revision is latest (0)", ErrorCodes::BAD_ARGUMENTS);

            /// This case complicates S3 cleanup in case of unsuccessful restore.
            if (information.source_path != disk->object_storage_root_path && disk->object_storage_root_path.starts_with(information.source_path))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Restoring to the same bucket is allowed only if source path is not a sub-path of configured path in S3 disk");
        }
        else
        {
            object_storage_from_another_namespace = disk->object_storage->cloneObjectStorage(information.source_namespace, config, config_prefix, context);
            source_object_storage = object_storage_from_another_namespace.get();
        }

        LOG_INFO(disk->log, "Starting to restore disk {}. Revision: {}, Source path: {}",
                 disk->name, information.revision, information.source_path);

        if (readSchemaVersion(source_object_storage, information.source_path) < RESTORABLE_SCHEMA_VERSION)
            throw Exception("Source bucket doesn't have restorable schema.", ErrorCodes::BAD_ARGUMENTS);

        LOG_INFO(disk->log, "Removing old metadata...");

        bool cleanup_s3 = information.source_path != disk->object_storage_root_path;
        for (const auto & root : data_roots)
            if (disk->exists(root))
                disk->removeSharedRecursive(root + '/', !cleanup_s3, {});

        LOG_INFO(disk->log, "Old metadata removed, restoring new one");
        restoreFiles(source_object_storage, information);
        restoreFileOperations(source_object_storage, information);

        auto tx = disk->metadata_storage->createTransaction();
        tx->unlinkFile(RESTORE_FILE_NAME);
        tx->commit();

        saveSchemaVersion(RESTORABLE_SCHEMA_VERSION);

        LOG_INFO(disk->log, "Restore disk {} finished", disk->name);
    }
    catch (const Exception &)
    {
        tryLogCurrentException(disk->log, fmt::format("Failed to restore disk {}", disk->name));

        throw;
    }
}

void DiskObjectStorageRemoteMetadataRestoreHelper::readRestoreInformation(RestoreInformation & restore_information) /// NOLINT
{
    auto metadata_str = disk->metadata_storage->readFileToString(RESTORE_FILE_NAME);
    ReadBufferFromString buffer(metadata_str);

    try
    {
        std::map<String, String> properties;

        while (buffer.hasPendingData())
        {
            String property;
            readText(property, buffer);
            assertChar('\n', buffer);

            auto pos = property.find('=');
            if (pos == std::string::npos || pos == 0 || pos == property.length())
                throw Exception(fmt::format("Invalid property {} in restore file", property), ErrorCodes::UNKNOWN_FORMAT);

            auto key = property.substr(0, pos);
            auto value = property.substr(pos + 1);

            auto it = properties.find(key);
            if (it != properties.end())
                throw Exception(fmt::format("Property key duplication {} in restore file", key), ErrorCodes::UNKNOWN_FORMAT);

            properties[key] = value;
        }

        for (const auto & [key, value] : properties)
        {
            ReadBufferFromString value_buffer(value);

            if (key == "revision")
                readIntText(restore_information.revision, value_buffer);
            else if (key == "source_bucket" || key == "source_namespace")
                readText(restore_information.source_namespace, value_buffer);
            else if (key == "source_path")
                readText(restore_information.source_path, value_buffer);
            else if (key == "detached")
                readBoolTextWord(restore_information.detached, value_buffer);
            else
                throw Exception(fmt::format("Unknown key {} in restore file", key), ErrorCodes::UNKNOWN_FORMAT);
        }
    }
    catch (const Exception &)
    {
        tryLogCurrentException(disk->log, "Failed to read restore information");
        throw;
    }
}

static String shrinkKey(const String & path, const String & key)
{
    if (!key.starts_with(path))
        throw Exception("The key " + key + " prefix mismatch with given " + path, ErrorCodes::LOGICAL_ERROR);

    return key.substr(path.length());
}

static std::tuple<UInt64, String> extractRevisionAndOperationFromKey(const String & key)
{
    String revision_str;
    String suffix;
    String operation;
    /// Key has format: ../../r{revision}(-{hostname})-{operation}
    static const re2::RE2 key_regexp{R"(.*/r(\d+)(-[\w\d\-\.]+)?-(\w+)$)"};

    re2::RE2::FullMatch(key, key_regexp, &revision_str, &suffix, &operation);

    return {(revision_str.empty() ? 0 : static_cast<UInt64>(std::bitset<64>(revision_str).to_ullong())), operation};
}

void DiskObjectStorageRemoteMetadataRestoreHelper::moveRecursiveOrRemove(const String & from_path, const String & to_path, bool send_metadata)
{
    if (disk->exists(to_path))
    {
        if (send_metadata)
        {
            auto revision = ++revision_counter;
            const ObjectAttributes object_metadata {
                {"from_path", from_path},
                {"to_path", to_path}
            };
            createFileOperationObject("rename", revision, object_metadata);
        }
        if (disk->isDirectory(from_path))
        {
            for (auto it = disk->iterateDirectory(from_path); it->isValid(); it->next())
                moveRecursiveOrRemove(it->path(), fs::path(to_path) / it->name(), false);
        }
        else
        {
            disk->removeFile(from_path);
        }
    }
    else
    {
        disk->moveFile(from_path, to_path, send_metadata);
    }
}

void DiskObjectStorageRemoteMetadataRestoreHelper::restoreFiles(IObjectStorage * source_object_storage, const RestoreInformation & restore_information)
{
    LOG_INFO(disk->log, "Starting restore files for disk {}", disk->name);

    std::vector<std::future<void>> results;
    auto restore_files = [this, &source_object_storage, &restore_information, &results](const RelativePathsWithSize & objects)
    {
        std::vector<String> keys_names;
        for (const auto & object : objects)
        {

            LOG_INFO(disk->log, "Calling restore for key for disk {}", object.relative_path);

            /// Skip file operations objects. They will be processed separately.
            if (object.relative_path.find("/operations/") != String::npos)
                continue;

            const auto [revision, _] = extractRevisionAndOperationFromKey(object.relative_path);
            /// Filter early if it's possible to get revision from key.
            if (revision > restore_information.revision)
                continue;

            keys_names.push_back(object.relative_path);
        }

        if (!keys_names.empty())
        {
            auto result = disk->getExecutor().execute([this, &source_object_storage, &restore_information, keys_names]()
            {
                processRestoreFiles(source_object_storage, restore_information.source_path, keys_names);
            });

            results.push_back(std::move(result));
        }

        return true;
    };

    RelativePathsWithSize children;
    source_object_storage->listPrefix(restore_information.source_path, children);

    restore_files(children);

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();

    LOG_INFO(disk->log, "Files are restored for disk {}", disk->name);

}

void DiskObjectStorageRemoteMetadataRestoreHelper::processRestoreFiles(
    IObjectStorage * source_object_storage, const String & source_path, const std::vector<String> & keys) const
{
    for (const auto & key : keys)
    {
        auto meta = source_object_storage->getObjectMetadata(key);
        auto object_attributes = meta.attributes;

        String path;
        if (object_attributes.has_value())
        {
            /// Restore file if object has 'path' in metadata.
            auto path_entry = object_attributes->find("path");
            if (path_entry == object_attributes->end())
            {
                /// Such keys can remain after migration, we can skip them.
                LOG_WARNING(disk->log, "Skip key {} because it doesn't have 'path' in metadata", key);
                continue;
            }

            path = path_entry->second;
        }
        else
            continue;

        disk->createDirectories(directoryPath(path));
        auto relative_key = shrinkKey(source_path, key);

        StoredObject object_from{key};
        StoredObject object_to{fs::path(disk->object_storage_root_path) / relative_key};

        /// Copy object if we restore to different bucket / path.
        if (source_object_storage->getObjectsNamespace() != disk->object_storage->getObjectsNamespace() || disk->object_storage_root_path != source_path)
            source_object_storage->copyObjectToAnotherObjectStorage(object_from, object_to, *disk->object_storage);

        auto tx = disk->metadata_storage->createTransaction();
        tx->addBlobToMetadata(path, relative_key, meta.size_bytes);
        tx->commit();

        LOG_TRACE(disk->log, "Restored file {}", path);
    }

}

void DiskObjectStorage::onFreeze(const String & path)
{
    createDirectories(path);
    auto tx =  metadata_storage->createTransaction();
    WriteBufferFromOwnString revision_file_buf ;
    writeIntText(metadata_helper->revision_counter.load(), revision_file_buf);
    tx->writeStringToFile(path + "revision.txt", revision_file_buf.str());
    tx->commit();
}

static String pathToDetached(const String & source_path)
{
    if (source_path.ends_with('/'))
        return fs::path(source_path).parent_path().parent_path() / "detached/";
    return fs::path(source_path).parent_path() / "detached/";
}

void DiskObjectStorageRemoteMetadataRestoreHelper::restoreFileOperations(IObjectStorage * source_object_storage, const RestoreInformation & restore_information)
{
    /// Enable recording file operations if we restore to different bucket / path.
    bool send_metadata = source_object_storage->getObjectsNamespace() != disk->object_storage->getObjectsNamespace()
        || disk->object_storage_root_path != restore_information.source_path;

    std::set<String> renames;
    auto restore_file_operations = [this, &source_object_storage, &restore_information, &renames, &send_metadata](const RelativePathsWithSize & objects)
    {
        const String rename = "rename";
        const String hardlink = "hardlink";

        for (const auto & object : objects)
        {
            const auto [revision, operation] = extractRevisionAndOperationFromKey(object.relative_path);
            if (revision == UNKNOWN_REVISION)
            {
                LOG_WARNING(disk->log, "Skip key {} with unknown revision", object.relative_path);
                continue;
            }

            /// S3 ensures that keys will be listed in ascending UTF-8 bytes order (revision order).
            /// We can stop processing if revision of the object is already more than required.
            if (revision > restore_information.revision)
                return false;

            /// Keep original revision if restore to different bucket / path.
            if (send_metadata)
                revision_counter = revision - 1;

            auto object_attributes = *(source_object_storage->getObjectMetadata(object.relative_path).attributes);
            if (operation == rename)
            {
                auto from_path = object_attributes["from_path"];
                auto to_path = object_attributes["to_path"];
                if (disk->exists(from_path))
                {
                    moveRecursiveOrRemove(from_path, to_path, send_metadata);

                    LOG_TRACE(disk->log, "Revision {}. Restored rename {} -> {}", revision, from_path, to_path);

                    if (restore_information.detached && disk->isDirectory(to_path))
                    {
                        /// Sometimes directory paths are passed without trailing '/'. We should keep them in one consistent way.
                        if (!from_path.ends_with('/'))
                            from_path += '/';
                        if (!to_path.ends_with('/'))
                            to_path += '/';

                        /// Always keep latest actual directory path to avoid 'detaching' not existing paths.
                        auto it = renames.find(from_path);
                        if (it != renames.end())
                            renames.erase(it);

                        renames.insert(to_path);
                    }
                }
            }
            else if (operation == hardlink)
            {
                auto src_path = object_attributes["src_path"];
                auto dst_path = object_attributes["dst_path"];
                if (disk->exists(src_path))
                {
                    disk->createDirectories(directoryPath(dst_path));
                    disk->createHardLink(src_path, dst_path, send_metadata);
                    LOG_TRACE(disk->log, "Revision {}. Restored hardlink {} -> {}", revision, src_path, dst_path);
                }
            }
        }

        return true;
    };

    RelativePathsWithSize children;
    source_object_storage->listPrefix(restore_information.source_path + "operations/", children);
    restore_file_operations(children);

    if (restore_information.detached)
    {
        Strings not_finished_prefixes{"tmp_", "delete_tmp_", "attaching_", "deleting_"};

        auto tx = disk->metadata_storage->createTransaction();
        for (const auto & path : renames)
        {
            /// Skip already detached parts.
            if (path.find("/detached/") != std::string::npos)
                continue;

            /// Skip not finished parts. They shouldn't be in 'detached' directory, because CH wouldn't be able to finish processing them.
            fs::path directory_path(path);
            auto directory_name = directory_path.parent_path().filename().string();

            auto predicate = [&directory_name](String & prefix) { return directory_name.starts_with(prefix); };
            if (std::any_of(not_finished_prefixes.begin(), not_finished_prefixes.end(), predicate))
                continue;

            auto detached_path = pathToDetached(path);

            LOG_TRACE(disk->log, "Move directory to 'detached' {} -> {}", path, detached_path);

            fs::path from_path = fs::path(path);
            fs::path to_path = fs::path(detached_path);
            if (path.ends_with('/'))
                to_path /= from_path.parent_path().filename();
            else
                to_path /= from_path.filename();

            /// to_path may exist and non-empty in case for example abrupt restart, so remove it before rename
            if (disk->metadata_storage->exists(to_path))
                tx->removeRecursive(to_path);

            disk->createDirectories(directoryPath(to_path));
            tx->moveDirectory(from_path, to_path);
        }
        tx->commit();
    }

    LOG_INFO(disk->log, "File operations restored for disk {}", disk->name);
}

}
