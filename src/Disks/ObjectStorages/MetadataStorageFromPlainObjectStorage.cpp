#include "MetadataStorageFromPlainObjectStorage.h"
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorageOperations.h>
#include <Disks/ObjectStorages/StaticDirectoryIterator.h>

#include <Common/filesystemHelpers.h>

#include <filesystem>
#include <tuple>

namespace DB
{

namespace
{

std::filesystem::path normalizeDirectoryPath(const std::filesystem::path & path)
{
    return path / "";
}

}

MetadataStorageFromPlainObjectStorage::MetadataStorageFromPlainObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_)
    : object_storage(object_storage_)
    , storage_path_prefix(std::move(storage_path_prefix_))
{
}

MetadataTransactionPtr MetadataStorageFromPlainObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainObjectStorageTransaction>(*this, object_storage);
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
    auto key_prefix = object_storage->generateObjectKeyForPath(path).serialize();
    auto directory = std::filesystem::path(std::move(key_prefix)) / "";

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
    auto key_prefix = object_storage->generateObjectKeyForPath(path).serialize();

    RelativePathsWithMetadata files;
    std::string abs_key = key_prefix;
    if (!abs_key.ends_with('/'))
        abs_key += '/';

    object_storage->listObjects(abs_key, files, 0);

    return getDirectChildrenOnDisk(abs_key, files, path);
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

std::vector<std::string> MetadataStorageFromPlainObjectStorage::getDirectChildrenOnDisk(
    const std::string & storage_key, const RelativePathsWithMetadata & remote_paths, const std::string & /* local_path */) const
{
    std::unordered_set<std::string> duplicates_filter;
    for (const auto & elem : remote_paths)
    {
        const auto & path = elem->relative_path;
        chassert(path.find(storage_key) == 0);
        const auto child_pos = storage_key.size();
        /// string::npos is ok.
        const auto slash_pos = path.find('/', child_pos);
        if (slash_pos == std::string::npos)
            duplicates_filter.emplace(path.substr(child_pos));
        else
            duplicates_filter.emplace(path.substr(child_pos, slash_pos - child_pos));
    }
    return std::vector<std::string>(std::make_move_iterator(duplicates_filter.begin()), std::make_move_iterator(duplicates_filter.end()));
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
    if (metadata_storage.object_storage->isWriteOnce())
    {
        for (auto it = metadata_storage.iterateDirectory(path); it->isValid(); it->next())
            metadata_storage.object_storage->removeObject(StoredObject(it->path()));
    }
    else
    {
        addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation>(
            normalizeDirectoryPath(path), *metadata_storage.getPathMap(), object_storage));
    }
}

void MetadataStorageFromPlainObjectStorageTransaction::createDirectory(const std::string & path)
{
    if (metadata_storage.object_storage->isWriteOnce())
        return;

    auto normalized_path = normalizeDirectoryPath(path);
    auto key_prefix = object_storage->generateObjectKeyPrefixForDirectoryPath(normalized_path).serialize();
    auto op = std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        std::move(normalized_path), std::move(key_prefix), *metadata_storage.getPathMap(), object_storage);
    addOperation(std::move(op));
}

void MetadataStorageFromPlainObjectStorageTransaction::createDirectoryRecursive(const std::string & path)
{
    createDirectory(path);
}

void MetadataStorageFromPlainObjectStorageTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    if (metadata_storage.object_storage->isWriteOnce())
        throwNotImplemented();

    addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(
        normalizeDirectoryPath(path_from), normalizeDirectoryPath(path_to), *metadata_storage.getPathMap(), object_storage));
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

void MetadataStorageFromPlainObjectStorageTransaction::commit()
{
    MetadataOperationsHolder::commitImpl(metadata_storage.metadata_mutex);
}
}
