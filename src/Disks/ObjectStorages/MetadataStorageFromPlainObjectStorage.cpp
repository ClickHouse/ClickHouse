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
    const std::string & object_storage_root_path_)
    : object_storage(object_storage_)
    , object_storage_root_path(object_storage_root_path_)
{
}

MetadataTransactionPtr MetadataStorageFromPlainObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainObjectStorageTransaction>(*this);
}

const std::string & MetadataStorageFromPlainObjectStorage::getPath() const
{
    return object_storage_root_path;
}
std::filesystem::path MetadataStorageFromPlainObjectStorage::getAbsolutePath(const std::string & path) const
{
    return fs::path(object_storage_root_path) / path;
}

bool MetadataStorageFromPlainObjectStorage::exists(const std::string & path) const
{
    /// NOTE: exists() cannot be used here since it works only for existing
    /// key, and does not work for some intermediate path.
    std::string abs_path = getAbsolutePath(path);
    return object_storage->existsOrHasAnyChild(abs_path);
}

bool MetadataStorageFromPlainObjectStorage::isFile(const std::string & path) const
{
    /// NOTE: This check is inaccurate and has excessive API calls
    return exists(path) && !isDirectory(path);
}

bool MetadataStorageFromPlainObjectStorage::isDirectory(const std::string & path) const
{
    std::string directory = getAbsolutePath(path);
    if (!directory.ends_with('/'))
        directory += '/';

    RelativePathsWithMetadata files;
    object_storage->listObjects(directory, files, 1);
    return !files.empty();
}

uint64_t MetadataStorageFromPlainObjectStorage::getFileSize(const String & path) const
{
    RelativePathsWithMetadata children;
    auto metadata = object_storage->tryGetObjectMetadata(getAbsolutePath(path));
    if (metadata)
        return metadata->size_bytes;
    return 0;
}

std::vector<std::string> MetadataStorageFromPlainObjectStorage::listDirectory(const std::string & path) const
{
    RelativePathsWithMetadata files;
    std::string abs_path = getAbsolutePath(path);
    if (!abs_path.ends_with('/'))
        abs_path += '/';

    object_storage->listObjects(abs_path, files, 0);

    std::vector<std::string> result;
    for (const auto & path_size : files)
    {
        result.push_back(path_size.relative_path);
    }

    std::unordered_set<std::string> duplicates_filter;
    for (auto & row : result)
    {
        chassert(row.starts_with(abs_path));
        row.erase(0, abs_path.size());
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
    std::vector<std::filesystem::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

StoredObjects MetadataStorageFromPlainObjectStorage::getStorageObjects(const std::string & path) const
{
    std::string blob_name = object_storage->generateBlobNameForPath(path);
    size_t object_size = getFileSize(blob_name);
    auto object = StoredObject(getAbsolutePath(blob_name), object_size, path);
    return {std::move(object)};
}

const IMetadataStorage & MetadataStorageFromPlainObjectStorageTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void MetadataStorageFromPlainObjectStorageTransaction::unlinkFile(const std::string & path)
{
    auto object = StoredObject(metadata_storage.getAbsolutePath(path));
    metadata_storage.object_storage->removeObject(object);
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
    const std::string &, const std::string & /* blob_name */, uint64_t /* size_in_bytes */)
{
    /// Noop, local metadata files is only one file, it is the metadata file itself.
}

UnlinkMetadataFileOperationOutcomePtr MetadataStorageFromPlainObjectStorageTransaction::unlinkMetadata(const std::string &)
{
    /// No hardlinks, so will always remove file.
    return std::make_shared<UnlinkMetadataFileOperationOutcome>(UnlinkMetadataFileOperationOutcome{0});
}

}
