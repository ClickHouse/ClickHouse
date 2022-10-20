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
    extern const int NOT_IMPLEMENTED;
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

bool MetadataStorageFromPlainObjectStorage::exists(const std::string & path) const
{
    auto object = StoredObject::create(*object_storage, fs::path(object_storage_root_path) / path);
    return object_storage->exists(object);
}

bool MetadataStorageFromPlainObjectStorage::isFile(const std::string & path) const
{
    /// NOTE: This check is inaccurate and has excessive API calls
    return !isDirectory(path) && exists(path);
}

bool MetadataStorageFromPlainObjectStorage::isDirectory(const std::string & path) const
{
    std::string directory = path;
    trimRight(directory);
    directory += "/";

    /// NOTE: This check is far from ideal, since it work only if the directory
    /// really has files, and has excessive API calls
    RelativePathsWithSize children;
    object_storage->listPrefix(directory, children);
    return !children.empty();
}

Poco::Timestamp MetadataStorageFromPlainObjectStorage::getLastModified(const std::string &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getLastModified is not implemented for MetadataStorageFromPlainObjectStorage");
}

struct stat MetadataStorageFromPlainObjectStorage::stat(const std::string &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "stat is not implemented for MetadataStorageFromPlainObjectStorage");
}

time_t MetadataStorageFromPlainObjectStorage::getLastChanged(const std::string &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getLastChanged is not implemented for MetadataStorageFromPlainObjectStorage");
}

uint64_t MetadataStorageFromPlainObjectStorage::getFileSize(const String & path) const
{
    RelativePathsWithSize children;
    object_storage->listPrefix(path, children);
    if (children.empty())
        return 0;
    if (children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "listPrefix() return multiple paths ({}) for {}", children.size(), path);
    return children.front().bytes_size;
}

std::vector<std::string> MetadataStorageFromPlainObjectStorage::listDirectory(const std::string & path) const
{
    RelativePathsWithSize children;
    object_storage->listPrefix(path, children);

    std::vector<std::string> result;
    for (const auto & path_size : children)
    {
        result.push_back(path_size.relative_path);
    }
    return result;
}

DirectoryIteratorPtr MetadataStorageFromPlainObjectStorage::iterateDirectory(const std::string & path) const
{
    /// NOTE: this is not required for BACKUP/RESTORE, but this is a first step
    /// towards MergeTree on plain S3.
    auto paths = listDirectory(path);
    std::vector<std::filesystem::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

std::string MetadataStorageFromPlainObjectStorage::readFileToString(const std::string &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "readFileToString is not implemented for MetadataStorageFromPlainObjectStorage");
}

std::unordered_map<String, String> MetadataStorageFromPlainObjectStorage::getSerializedMetadata(const std::vector<String> &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getSerializedMetadata is not implemented for MetadataStorageFromPlainObjectStorage");
}

StoredObjects MetadataStorageFromPlainObjectStorage::getStorageObjects(const std::string & path) const
{
    std::string blob_name = object_storage->generateBlobNameForPath(path);

    std::string object_path = fs::path(object_storage_root_path) / blob_name;
    size_t object_size = getFileSize(object_path);

    auto object = StoredObject::create(*object_storage, object_path, object_size, /* exists */true);
    return {std::move(object)};
}

uint32_t MetadataStorageFromPlainObjectStorage::getHardlinkCount(const std::string &) const
{
    return 1;
}

const IMetadataStorage & MetadataStorageFromPlainObjectStorageTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void MetadataStorageFromPlainObjectStorageTransaction::writeStringToFile(const std::string &, const std::string & /* data */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "writeStringToFile is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::setLastModified(const std::string &, const Poco::Timestamp &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "setLastModified is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::unlinkFile(const std::string & path)
{
    auto object = StoredObject::create(*metadata_storage.object_storage, fs::path(metadata_storage.object_storage_root_path) / path);
    metadata_storage.object_storage->removeObject(object);
}

void MetadataStorageFromPlainObjectStorageTransaction::removeRecursive(const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "removeRecursive is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::createDirectory(const std::string &)
{
    /// Noop. It is an Object Storage not a filesystem.
}

void MetadataStorageFromPlainObjectStorageTransaction::createDirectoryRecursive(const std::string &)
{
    /// Noop. It is an Object Storage not a filesystem.
}

void MetadataStorageFromPlainObjectStorageTransaction::removeDirectory(const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "removeDirectory is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::moveFile(const std::string & /* path_from */, const std::string & /* path_to */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "moveFile is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::moveDirectory(const std::string & /* path_from */, const std::string & /* path_to */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "moveDirectory is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::replaceFile(const std::string & /* path_from */, const std::string & /* path_to */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "replaceFile is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::chmod(const String &, mode_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "chmod is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::setReadOnly(const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "setReadOnly is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::createHardLink(const std::string & /* path_from */, const std::string & /* path_to */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "createHardLink is not implemented for MetadataStorageFromPlainObjectStorage");
}

void MetadataStorageFromPlainObjectStorageTransaction::createEmptyMetadataFile(const std::string &)
{
    /// Noop, no separate metadata.
}

void MetadataStorageFromPlainObjectStorageTransaction::createMetadataFile(
    const std::string &, const std::string & /* blob_name */, uint64_t /* size_in_bytes */)
{
    /// Noop, no separate metadata.
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
