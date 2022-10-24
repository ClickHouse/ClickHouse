#include "MetadataStorageFromPlainObjectStorage.h"
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/StaticDirectoryIterator.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MetadataStorageFromPlainObjectStorage::MetadataStorageFromPlainObjectStorage(
    ObjectStoragePtr object_storage_,
    const std::string & object_storage_root_path_)
    : object_storage(object_storage_)
    , object_storage_root_path(object_storage_root_path_)
{
}

MetadataTransactionPtr MetadataStorageFromPlainObjectStorage::createTransaction() const
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
    auto object = StoredObject::create(*object_storage, getAbsolutePath(path));
    return object_storage->exists(object);
}

bool MetadataStorageFromPlainObjectStorage::isFile(const std::string & path) const
{
    /// NOTE: This check is inaccurate and has excessive API calls
    return exists(path) && !isDirectory(path);
}

bool MetadataStorageFromPlainObjectStorage::isDirectory(const std::string & path) const
{
    std::string directory = getAbsolutePath(path);
    trimRight(directory);
    directory += "/";

    /// NOTE: This check is far from ideal, since it work only if the directory
    /// really has files, and has excessive API calls
    RelativePathsWithSize children;
    object_storage->listPrefix(directory, children);
    return !children.empty();
}

uint64_t MetadataStorageFromPlainObjectStorage::getFileSize(const String & path) const
{
    RelativePathsWithSize children;
    object_storage->listPrefix(getAbsolutePath(path), children);
    if (children.empty())
        return 0;
    if (children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "listPrefix() return multiple paths ({}) for {}", children.size(), path);
    return children.front().bytes_size;
}

std::vector<std::string> MetadataStorageFromPlainObjectStorage::listDirectory(const std::string & path) const
{
    RelativePathsWithSize children;
    std::vector<std::string> common_prefixes;
    object_storage->listPrefixInPath(getAbsolutePath(path), children, common_prefixes);

    std::vector<std::string> result;
    for (const auto & path_size : children)
        result.push_back(path_size.relative_path);
    for (const auto & common_prefix : common_prefixes)
        result.push_back(common_prefix);
    return result;
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
    auto object = StoredObject::create(*object_storage, getAbsolutePath(blob_name), object_size, /* exists */true);
    return {std::move(object)};
}

const IMetadataStorage & MetadataStorageFromPlainObjectStorageTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void MetadataStorageFromPlainObjectStorageTransaction::unlinkFile(const std::string & path)
{
    auto object = StoredObject::create(*metadata_storage.object_storage, metadata_storage.getAbsolutePath(path));
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
void MetadataStorageFromPlainObjectStorageTransaction::unlinkMetadata(const std::string &)
{
    /// Noop, no separate metadata.
}

}
