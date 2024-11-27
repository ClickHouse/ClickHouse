#include "MetadataStorageFromPlainObjectStorage.h"
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/StaticDirectoryIterator.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>


namespace DB
{

MetadataStorageFromPlainObjectStorage::MetadataStorageFromPlainObjectStorage(
    ObjectStoragePtr object_storage_,
    String storage_path_prefix_)
    : object_storage(object_storage_)
    , storage_path_prefix(std::move(storage_path_prefix_))
{
}

MetadataTransactionPtr MetadataStorageFromPlainObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainObjectStorageTransaction>(*this);
}

const std::string & MetadataStorageFromPlainObjectStorage::getPath() const
{
    return storage_path_prefix;
}

bool MetadataStorageFromPlainObjectStorage::exists(const std::string & path) const
{
    /// NOTE: exists() cannot be used here since it works only for existing
    /// key, and does not work for some intermediate path.
    auto object_key = object_storage->generateObjectKeyForPath(path);
    return object_storage->existsOrHasAnyChild(object_key.serialize());
}

bool MetadataStorageFromPlainObjectStorage::isFile(const std::string & path) const
{
    /// NOTE: This check is inaccurate and has excessive API calls
    return exists(path) && !isDirectory(path);
}

bool MetadataStorageFromPlainObjectStorage::isDirectory(const std::string & path) const
{
    auto object_key = object_storage->generateObjectKeyForPath(path);
    std::string directory = object_key.serialize();
    if (!directory.ends_with('/'))
        directory += '/';
    return object_storage->existsOrHasAnyChild(directory);
}

uint64_t MetadataStorageFromPlainObjectStorage::getFileSize(const String & path) const
{
    auto object_key = object_storage->generateObjectKeyForPath(path);
    auto metadata = object_storage->tryGetObjectMetadata(object_key.serialize());
    if (metadata)
        return metadata->size_bytes;
    return 0;
}

std::vector<std::string> MetadataStorageFromPlainObjectStorage::listDirectory(const std::string & path) const
{
    auto object_key = object_storage->generateObjectKeyForPath(path);

    RelativePathsWithMetadata files;
    std::string abs_key = object_key.serialize();
    if (!abs_key.ends_with('/'))
        abs_key += '/';

    object_storage->listObjects(abs_key, files, 0);

    std::vector<std::string> result;
    for (const auto & path_size : files)
    {
        result.push_back(path_size.relative_path);
    }

    std::unordered_set<std::string> duplicates_filter;
    for (auto & row : result)
    {
        chassert(row.starts_with(abs_key));
        row.erase(0, abs_key.size());
        auto slash_pos = row.find_first_of('/');
        if (slash_pos != std::string::npos)
            row.erase(slash_pos, row.size() - slash_pos);
        duplicates_filter.insert(row);
    }

    return std::vector<std::string>(duplicates_filter.begin(), duplicates_filter.end());
}

DirectoryIteratorPtr MetadataStorageFromPlainObjectStorage::iterateDirectory(const std::string & path) const
{
    /// Required for MergeTree
    auto paths = listDirectory(path);
    // Prepend path, since iterateDirectory() includes path, unlike listDirectory()
    std::for_each(paths.begin(), paths.end(), [&](auto & child) { child = fs::path(path) / child; });
    std::vector<std::filesystem::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

StoredObjects MetadataStorageFromPlainObjectStorage::getStorageObjects(const std::string & path) const
{
    size_t object_size = getFileSize(path);
    auto object_key = object_storage->generateObjectKeyForPath(path);
    return {StoredObject(object_key.serialize(), path, object_size)};
}

const IMetadataStorage & MetadataStorageFromPlainObjectStorageTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void MetadataStorageFromPlainObjectStorageTransaction::unlinkFile(const std::string & path)
{
    auto object_key = metadata_storage.object_storage->generateObjectKeyForPath(path);
    auto object = StoredObject(object_key.serialize());
    metadata_storage.object_storage->removeObject(object);
}

void MetadataStorageFromPlainObjectStorageTransaction::removeDirectory(const std::string & path)
{
    for (auto it = metadata_storage.iterateDirectory(path); it->isValid(); it->next())
        metadata_storage.object_storage->removeObject(StoredObject(it->path()));
}

void MetadataStorageFromPlainObjectStorageTransaction::createDirectory(const std::string &)
{
    /// Noop. It is an Object Storage not a filesystem.
}
void MetadataStorageFromPlainObjectStorageTransaction::createDirectoryRecursive(const std::string &)
{
    /// Noop. It is an Object Storage not a filesystem.
}
void MetadataStorageFromPlainObjectStorageTransaction::addBlobToMetadata(
    const std::string &, ObjectStorageKey /* object_key */, uint64_t /* size_in_bytes */)
{
    /// Noop, local metadata files is only one file, it is the metadata file itself.
}

UnlinkMetadataFileOperationOutcomePtr MetadataStorageFromPlainObjectStorageTransaction::unlinkMetadata(const std::string &)
{
    /// No hardlinks, so will always remove file.
    return std::make_shared<UnlinkMetadataFileOperationOutcome>(UnlinkMetadataFileOperationOutcome{0});
}

}
