#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageFromMemory.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MetadataStorageFromMemory::MetadataStorageFromMemory(std::string compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_)
    : compatible_key_prefix(std::move(compatible_key_prefix_))
    , key_generator(std::move(key_generator_))
{
}

void MetadataStorageFromMemory::assertExists(const std::string & path) const
{
    if (!existsFile(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
}

const std::string & MetadataStorageFromMemory::getPath() const
{
    static const String empty;
    return empty;
}

bool MetadataStorageFromMemory::existsFile(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return tree.existsFile(normalizePath(path));
}

bool MetadataStorageFromMemory::existsDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return tree.existsDirectory(normalizePath(path));
}

bool MetadataStorageFromMemory::existsFileOrDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return tree.existsFileOrDirectory(normalizePath(path));
}

Poco::Timestamp MetadataStorageFromMemory::getLastModified(const std::string & path) const
{
    assertExists(path);
    return {};
}

time_t MetadataStorageFromMemory::getLastChanged(const std::string & path) const
{
    assertExists(path);
    return {};
}

const DiskObjectStorageMetadata & MetadataStorageFromMemory::getRecordUnlocked(const std::string & path) const
{
    const auto * record = tree.getMetadata(normalizePath(path));
    if (!record)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    return *record;
}

uint64_t MetadataStorageFromMemory::getFileSize(const String & path) const
{
    std::shared_lock lock(metadata_mutex);
    const auto & record = getRecordUnlocked(path);
    return record.objects.empty() ? record.inline_data.size() : getTotalSize(record.objects);
}

uint32_t MetadataStorageFromMemory::getHardlinkCount(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return static_cast<uint32_t>(getRecordUnlocked(path).ref_count);
}

StoredObjects MetadataStorageFromMemory::getStorageObjects(const std::string & path) const
{
    return readMetadata(path)->objects;
}

std::vector<std::string> MetadataStorageFromMemory::listDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return tree.listDirectory(normalizePath(path));
}

DirectoryIteratorPtr MetadataStorageFromMemory::iterateDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto children = tree.listDirectory(normalizePath(path));
    return std::make_unique<StaticDirectoryIterator>(std::vector<std::filesystem::path>{children.begin(), children.end()});
}

MetadataTransactionPtr MetadataStorageFromMemory::createTransaction()
{
    return std::make_shared<MetadataStorageFromMemoryTransaction>(*this);
}

DiskObjectStorageMetadataPtr MetadataStorageFromMemory::readMetadataUnlocked(const std::string & path) const
{
    const auto & record = getRecordUnlocked(path);

    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    metadata->objects = record.objects;
    metadata->inline_data = record.inline_data;
    metadata->ref_count = 1;
    metadata->read_only = true;

    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromMemory::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path);
}

std::string MetadataStorageFromMemory::readFileToString(const std::string & path) const
{
    return readMetadata(path)->serializeToString();
}

std::string MetadataStorageFromMemory::readInlineDataToString(const std::string & path) const
{
    return readMetadata(path)->inline_data;
}

std::unordered_map<String, String> MetadataStorageFromMemory::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    std::shared_lock lock(metadata_mutex);
    std::unordered_map<String, String> metadatas;

    for (const auto & path : file_paths)
    {
        auto metadata = readMetadataUnlocked(path);
        metadata->ref_count = 0;
        WriteBufferFromOwnString buf;
        metadata->serialize(buf);
        metadatas[path] = buf.str();
    }

    return metadatas;
}

struct stat MetadataStorageFromMemory::stat(const String &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "stat() method is not implemented for MetadataStorageFromMemory");
}

DiskObjectStorageMetadata & MetadataStorageFromMemory::findRecordOfBlobUnlocked(const std::string & remote_path)
{
    DiskObjectStorageMetadata * found = nullptr;
    tree.forEachMetadataUnder({}, [&](const std::string &, DiskObjectStorageMetadata & record)
    {
        for (const auto & object : record.objects)
        {
            if (object.remote_path == remote_path)
            {
                /// A blob belongs to exactly one record within the storage.
                chassert(!found);
                found = &record;
            }
        }
    });

    if (!found)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No file record references blob `{}`", remote_path);
    return *found;
}

bool MetadataStorageFromMemory::hasTransientBuildState() const
{
    std::shared_lock lock(metadata_mutex);

    bool found = false;
    tree.forEachMetadataUnder({}, [&](const std::string &, DiskObjectStorageMetadata & record)
    {
        found = found || record.ref_count > 0;
    });
    return found;
}

/// MetadataStorageFromMemoryTransaction

void MetadataStorageFromMemoryTransaction::releaseRecordUnlocked(const DiskObjectStorageMetadata & metadata)
{
    /// A positive count means the blob is referenced elsewhere and must not be deleted.
    if (metadata.ref_count > 0)
        return;

    for (const auto & object : metadata.objects)
        pending_own_removals.push_back(object.remote_path);
}

void MetadataStorageFromMemoryTransaction::putRecordUnlocked(const std::string & path, DiskObjectStorageMetadata metadata)
{
    chassert(metadata.objects.empty() || metadata.inline_data.empty());

    if (auto displaced = storage.tree.putFile(normalizePath(path), std::move(metadata)))
        releaseRecordUnlocked(*displaced);
}

std::vector<String> MetadataStorageFromMemoryTransaction::takePendingOwnRemovals()
{
    return std::exchange(pending_own_removals, {});
}

std::unordered_map<String, Locations> MetadataStorageFromMemoryTransaction::takeReplicationRecords()
{
    return std::exchange(replication_records, {});
}

void MetadataStorageFromMemoryTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    /// Every operation is already applied; external commit options are not supported.
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "In-memory metadata transaction cannot carry external commit options");

    /// The storage has no removal queue, so nothing would ever dispose of the blobs this
    /// transaction released: the owner must drain `takePendingOwnRemovals` (and the replication
    /// records) before the commit, or the blobs leak silently.
    if (!pending_own_removals.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "In-memory metadata transaction is committed with {} released blobs not taken", pending_own_removals.size());
    if (!replication_records.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "In-memory metadata transaction is committed with {} replication records not taken", replication_records.size());
}

TransactionCommitOutcomeVariant MetadataStorageFromMemoryTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    commit(options);
    return true;
}

void MetadataStorageFromMemoryTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    std::unique_lock lock(storage.metadata_mutex);

    DiskObjectStorageMetadata record(storage.compatible_key_prefix, path);
    record.objects = objects;

    putRecordUnlocked(path, std::move(record));
}

void MetadataStorageFromMemoryTransaction::writeInlineDataToFile(const std::string & path, const std::string & data)
{
    std::unique_lock lock(storage.metadata_mutex);

    DiskObjectStorageMetadata record(storage.compatible_key_prefix, path);
    record.inline_data = data;

    putRecordUnlocked(path, std::move(record));
}

void MetadataStorageFromMemoryTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    std::unique_lock lock(storage.metadata_mutex);

    if (!storage.tree.existsFile(normalizePath(path)))
    {
        if (if_exists)
            return;
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    }

    auto record = storage.tree.removeFile(normalizePath(path));
    if (should_remove_objects)
        releaseRecordUnlocked(record);
}

void MetadataStorageFromMemoryTransaction::createDirectory(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);
    storage.tree.createDirectory(normalizePath(path));
}

void MetadataStorageFromMemoryTransaction::createDirectoryRecursive(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);
    storage.tree.createDirectoryRecursive(normalizePath(path));
}

void MetadataStorageFromMemoryTransaction::removeDirectory(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);
    storage.tree.removeDirectory(normalizePath(path));
}

void MetadataStorageFromMemoryTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    std::unique_lock lock(storage.metadata_mutex);

    /// The predicate receives paths relative to the removed root, same as the on-disk backend
    /// (callers fill their keep-lists with relative names).
    storage.tree.removeSubtree(normalizePath(path), [&](const std::string & relative_path, DiskObjectStorageMetadata & record)
    {
        if (!should_remove_objects || should_remove_objects(relative_path))
            releaseRecordUnlocked(record);
    });
}

void MetadataStorageFromMemoryTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    storage.tree.moveFile(normalizePath(path_from), normalizePath(path_to), /*replace=*/false);
    for (auto & object : storage.tree.getMetadata(normalizePath(path_to))->objects)
        object.local_path = path_to;
}

void MetadataStorageFromMemoryTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    /// Overwrite semantics: the displaced destination record's sole-owner blobs are queued
    /// for disposal.
    if (auto displaced = storage.tree.moveFile(normalizePath(path_from), normalizePath(path_to), /*replace=*/true))
        releaseRecordUnlocked(*displaced);
    for (auto & object : storage.tree.getMetadata(normalizePath(path_to))->objects)
        object.local_path = path_to;
}

void MetadataStorageFromMemoryTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    storage.tree.moveDirectory(normalizePath(path_from), normalizePath(path_to));
    storage.tree.forEachMetadataUnder(normalizePath(path_to), [](const std::string & full_path, DiskObjectStorageMetadata & record)
    {
        for (auto & object : record.objects)
            object.local_path = full_path;
    });
}

void MetadataStorageFromMemoryTransaction::createHardLink(const std::string & /*path_from*/, const std::string & /*path_to*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Hardlinks are not supported by in-memory metadata storage");
}

void MetadataStorageFromMemoryTransaction::setLastModified(const std::string & /*path*/, const Poco::Timestamp & /*timestamp*/)
{
}

ObjectStorageKey MetadataStorageFromMemoryTransaction::generateObjectKeyForPath(const std::string & /*path*/)
{
    if (!storage.key_generator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Memory metadata storage has no object storage key generator");
    return storage.key_generator->generate();
}

void MetadataStorageFromMemoryTransaction::recordBlobsReplication(const StoredObject & blob, const Locations & missing_locations)
{
    if (missing_locations.empty())
        return;

    /// A blob is written and recorded once; a repeated record carries the same set.
    replication_records[blob.remote_path] = missing_locations;
}

StoredObjects MetadataStorageFromMemoryTransaction::getSubmittedForRemovalBlobs()
{
    return {};
}

void MetadataStorageFromMemoryTransaction::incrementBlobRefCount(const std::string & blob)
{
    std::unique_lock lock(storage.metadata_mutex);

    ++storage.findRecordOfBlobUnlocked(blob).ref_count;
}

void MetadataStorageFromMemoryTransaction::decrementBlobRefCount(const std::string & blob)
{
    std::unique_lock lock(storage.metadata_mutex);

    auto & record = storage.findRecordOfBlobUnlocked(blob);
    if (record.ref_count == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reference count of blob `{}` is already zero", blob);
    --record.ref_count;
}

}
