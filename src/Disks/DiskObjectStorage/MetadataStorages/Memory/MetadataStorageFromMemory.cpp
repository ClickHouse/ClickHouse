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
    return tree.existsFile(path);
}

bool MetadataStorageFromMemory::existsDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return tree.existsDirectory(path);
}

bool MetadataStorageFromMemory::existsFileOrDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return tree.existsFileOrDirectory(path);
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
    const auto * record = tree.getRecord(path);
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
    return tree.listDirectory(path);
}

DirectoryIteratorPtr MetadataStorageFromMemory::iterateDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto children = tree.listDirectory(path);
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

void MetadataStorageFromMemory::releaseRecordUnlocked(const DiskObjectStorageMetadata & record)
{
    /// A positive count means the blob is referenced elsewhere and must not be deleted.
    if (record.ref_count > 0)
        return;

    for (const auto & object : record.objects)
        pending_own_removals.push_back(object.remote_path);
}

void MetadataStorageFromMemory::putRecordUnlocked(const std::string & path, DiskObjectStorageMetadata record)
{
    chassert(record.objects.empty() || record.inline_data.empty());

    if (auto displaced = tree.putFile(path, std::move(record)))
        releaseRecordUnlocked(*displaced);
}

DiskObjectStorageMetadata & MetadataStorageFromMemory::findRecordOfBlobUnlocked(const std::string & remote_path)
{
    DiskObjectStorageMetadata * found = nullptr;
    tree.forEachRecordUnder("", [&](const std::string &, DiskObjectStorageMetadata & record)
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

std::vector<String> MetadataStorageFromMemory::takePendingOwnRemovals()
{
    std::unique_lock lock(metadata_mutex);
    return std::exchange(pending_own_removals, {});
}

std::unordered_map<String, Locations> MetadataStorageFromMemory::takeReplicationRecords()
{
    std::unique_lock lock(metadata_mutex);
    return std::exchange(replication_records, {});
}

bool MetadataStorageFromMemory::hasTransientBuildState() const
{
    std::shared_lock lock(metadata_mutex);

    if (!pending_own_removals.empty() || !replication_records.empty())
        return true;

    bool found = false;
    tree.forEachRecordUnder("", [&](const std::string &, DiskObjectStorageMetadata & record)
    {
        found = found || record.ref_count > 0;
    });
    return found;
}

/// MetadataStorageFromMemoryTransaction

void MetadataStorageFromMemoryTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    /// Every operation is already applied; external commit options are not supported.
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "In-memory metadata transaction cannot carry external commit options");
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

    storage.putRecordUnlocked(path, std::move(record));
}

void MetadataStorageFromMemoryTransaction::writeInlineDataToFile(const std::string & path, const std::string & data)
{
    std::unique_lock lock(storage.metadata_mutex);

    DiskObjectStorageMetadata record(storage.compatible_key_prefix, path);
    record.inline_data = data;

    storage.putRecordUnlocked(path, std::move(record));
}

void MetadataStorageFromMemoryTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    std::unique_lock lock(storage.metadata_mutex);

    if (!storage.tree.existsFile(path))
    {
        if (if_exists)
            return;
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    }

    auto record = storage.tree.removeFile(path);
    if (should_remove_objects)
        storage.releaseRecordUnlocked(record);
}

void MetadataStorageFromMemoryTransaction::createDirectory(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);
    storage.tree.createDirectory(path);
}

void MetadataStorageFromMemoryTransaction::createDirectoryRecursive(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);
    storage.tree.createDirectoryRecursive(path);
}

void MetadataStorageFromMemoryTransaction::removeDirectory(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);
    storage.tree.removeDirectory(path);
}

void MetadataStorageFromMemoryTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    std::unique_lock lock(storage.metadata_mutex);

    /// The predicate receives paths relative to the removed root, same as the on-disk backend
    /// (callers fill their keep-lists with relative names).
    storage.tree.removeSubtree(path, [&](const std::string & relative_path, DiskObjectStorageMetadata & record)
    {
        if (!should_remove_objects || should_remove_objects(relative_path))
            storage.releaseRecordUnlocked(record);
    });
}

void MetadataStorageFromMemoryTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    storage.tree.moveFile(path_from, path_to, /*replace=*/false);
    for (auto & object : storage.tree.getRecord(path_to)->objects)
        object.local_path = path_to;
}

void MetadataStorageFromMemoryTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    /// Overwrite semantics: the displaced destination record's sole-owner blobs are queued
    /// for disposal.
    if (auto displaced = storage.tree.moveFile(path_from, path_to, /*replace=*/true))
        storage.releaseRecordUnlocked(*displaced);
    for (auto & object : storage.tree.getRecord(path_to)->objects)
        object.local_path = path_to;
}

void MetadataStorageFromMemoryTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    storage.tree.moveDirectory(path_from, path_to);
    storage.tree.forEachRecordUnder(path_to, [](const std::string & full_path, DiskObjectStorageMetadata & record)
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

    std::unique_lock lock(storage.metadata_mutex);
    auto & locations = storage.replication_records[blob.remote_path];
    locations.insert(locations.end(), missing_locations.begin(), missing_locations.end());
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
