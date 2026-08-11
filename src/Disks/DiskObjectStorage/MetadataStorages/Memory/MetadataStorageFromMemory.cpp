#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageFromMemory.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

std::string_view normalizePath(std::string_view path)
{
    if (!path.empty() && path.ends_with('/'))
        return path.substr(0, path.size() - 1);
    return path;
}

/// "" (the root) prefixes everything; other directories prefix their subtree.
std::string directoryPrefix(std::string_view directory)
{
    auto normalized = normalizePath(directory);
    if (normalized.empty())
        return "";
    return std::string(normalized) + "/";
}

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
    return files.contains(path);
}

bool MetadataStorageFromMemory::existsDirectoryUnlocked(const std::string & path) const
{
    const std::string prefix = directoryPrefix(path);
    if (auto it = files.lower_bound(prefix); it != files.end() && it->first.starts_with(prefix) && it->first.size() > prefix.size())
        return true;

    if (directories.contains(std::string(normalizePath(path))))
        return true;

    auto dir_it = directories.lower_bound(prefix);
    return dir_it != directories.end() && dir_it->starts_with(prefix);
}

bool MetadataStorageFromMemory::existsDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return existsDirectoryUnlocked(path);
}

bool MetadataStorageFromMemory::existsFileOrDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return files.contains(std::string(normalizePath(path))) || existsDirectoryUnlocked(path);
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

uint64_t MetadataStorageFromMemory::getFileSize(const String & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto it = files.find(path);
    if (it == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    return it->second.objects.empty() ? it->second.inline_data.size() : getTotalSize(it->second.objects);
}

uint32_t MetadataStorageFromMemory::getHardlinkCount(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto it = files.find(path);
    if (it == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    return static_cast<uint32_t>(it->second.ref_count);
}

StoredObjects MetadataStorageFromMemory::getStorageObjects(const std::string & path) const
{
    return readMetadata(path)->objects;
}

std::vector<std::string> MetadataStorageFromMemory::listDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    const std::string prefix = directoryPrefix(path);

    std::set<std::string> children;

    for (auto it = files.lower_bound(prefix); it != files.end() && it->first.starts_with(prefix); ++it)
    {
        std::string_view suffix = std::string_view(it->first).substr(prefix.size());
        children.emplace(prefix + std::string(suffix.substr(0, std::min(suffix.find('/'), suffix.size()))));
    }

    for (auto it = directories.lower_bound(prefix); it != directories.end() && it->starts_with(prefix); ++it)
    {
        std::string_view suffix = std::string_view(*it).substr(prefix.size());
        if (suffix.empty())
            continue;
        children.emplace(prefix + std::string(suffix.substr(0, std::min(suffix.find('/'), suffix.size()))));
    }

    return {children.begin(), children.end()};
}

DirectoryIteratorPtr MetadataStorageFromMemory::iterateDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    const std::string prefix = directoryPrefix(path);

    std::set<std::string> children;

    for (auto it = files.lower_bound(prefix); it != files.end() && it->first.starts_with(prefix); ++it)
    {
        std::string_view suffix = std::string_view(it->first).substr(prefix.size());
        children.emplace(prefix + std::string(suffix.substr(0, std::min(suffix.find('/'), suffix.size()))));
    }

    for (auto it = directories.lower_bound(prefix); it != directories.end() && it->starts_with(prefix); ++it)
    {
        std::string_view suffix = std::string_view(*it).substr(prefix.size());
        if (suffix.empty())
            continue;
        children.emplace(prefix + std::string(suffix.substr(0, std::min(suffix.find('/'), suffix.size()))));
    }

    return std::make_unique<StaticDirectoryIterator>(std::vector<std::filesystem::path>{children.begin(), children.end()});
}

MetadataTransactionPtr MetadataStorageFromMemory::createTransaction()
{
    return std::make_shared<MetadataStorageFromMemoryTransaction>(*this);
}

DiskObjectStorageMetadataPtr MetadataStorageFromMemory::readMetadataUnlocked(const std::string & path) const
{
    auto it = files.find(path);
    if (it == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);

    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    metadata->objects = it->second.objects;
    metadata->inline_data = it->second.inline_data;
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

    if (auto it = files.find(path); it != files.end())
    {
        releaseRecordUnlocked(it->second);
        files.erase(it);
    }

    files.emplace(path, std::move(record));
}

DiskObjectStorageMetadata & MetadataStorageFromMemory::findRecordOfBlobUnlocked(const std::string & remote_path)
{
    DiskObjectStorageMetadata * found = nullptr;
    for (auto & [path, record] : files)
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
    }

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

    for (const auto & [path, record] : files)
        if (record.ref_count > 0)
            return true;

    return false;
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

    auto it = storage.files.find(path);
    if (it == storage.files.end())
    {
        if (if_exists)
            return;
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    }

    if (should_remove_objects)
        storage.releaseRecordUnlocked(it->second);

    storage.files.erase(it);
}

void MetadataStorageFromMemoryTransaction::createDirectory(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);
    storage.directories.emplace(normalizePath(path));
}

void MetadataStorageFromMemoryTransaction::createDirectoryRecursive(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);

    std::string_view remaining = normalizePath(path);
    while (!remaining.empty())
    {
        storage.directories.emplace(remaining);
        auto slash = remaining.rfind('/');
        if (slash == std::string_view::npos)
            break;
        remaining = remaining.substr(0, slash);
    }
}

void MetadataStorageFromMemoryTransaction::removeDirectory(const std::string & path)
{
    std::unique_lock lock(storage.metadata_mutex);

    const std::string prefix = directoryPrefix(path);
    if (auto it = storage.files.lower_bound(prefix); it != storage.files.end() && it->first.starts_with(prefix))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove directory `{}`: file `{}` remains under it", path, it->first);

    if (auto it = storage.directories.lower_bound(prefix); it != storage.directories.end() && it->starts_with(prefix))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove directory `{}`: directory `{}` remains under it", path, *it);

    storage.directories.erase(std::string(normalizePath(path)));
}

void MetadataStorageFromMemoryTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    std::unique_lock lock(storage.metadata_mutex);

    const std::string normalized(normalizePath(path));
    const std::string prefix = directoryPrefix(normalized);

    if (auto it = storage.files.find(normalized); it != storage.files.end())
    {
        if (!should_remove_objects || should_remove_objects(it->first))
            storage.releaseRecordUnlocked(it->second);
        storage.files.erase(it);
    }

    for (auto it = storage.files.lower_bound(prefix); it != storage.files.end() && it->first.starts_with(prefix);)
    {
        if (!should_remove_objects || should_remove_objects(it->first))
            storage.releaseRecordUnlocked(it->second);
        it = storage.files.erase(it);
    }

    storage.directories.erase(normalized);
    for (auto it = storage.directories.lower_bound(prefix); it != storage.directories.end() && it->starts_with(prefix);)
        it = storage.directories.erase(it);
}

void MetadataStorageFromMemoryTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    auto it = storage.files.find(path_from);
    if (it == storage.files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path_from);

    if (storage.files.contains(path_to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File `{}` already exists", path_to);

    auto node = storage.files.extract(it);
    node.key() = path_to;
    for (auto & object : node.mapped().objects)
        object.local_path = path_to;
    storage.files.insert(std::move(node));
}

void MetadataStorageFromMemoryTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    moveFile(path_from, path_to);
}

void MetadataStorageFromMemoryTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    const std::string from(normalizePath(path_from));
    const std::string to(normalizePath(path_to));
    const std::string from_prefix = directoryPrefix(from);
    const std::string to_prefix = directoryPrefix(to);

    if (!storage.existsDirectoryUnlocked(from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Directory `{}` doesn't exist", from);

    if (storage.existsDirectoryUnlocked(to) || storage.files.contains(to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File or directory `{}` already exists", to);

    auto rekey = [&](auto & container, bool include_exact)
    {
        auto get_key = [](const auto & entry) -> const std::string *
        {
            if constexpr (requires { entry.first; })
                return &entry.first;
            else
                return &entry;
        };

        std::vector<std::string> keys_to_move;
        if (include_exact && container.contains(from))
            keys_to_move.push_back(from);
        for (auto it = container.lower_bound(from_prefix); it != container.end() && get_key(*it)->starts_with(from_prefix); ++it)
            keys_to_move.push_back(*get_key(*it));

        for (const auto & key : keys_to_move)
        {
            auto node = container.extract(key);
            std::string new_key = to + key.substr(from.size());
            if constexpr (requires { node.key(); })
                node.key() = std::move(new_key);
            else
                node.value() = std::move(new_key);
            container.insert(std::move(node));
        }
    };

    rekey(storage.files, /*include_exact=*/false);
    rekey(storage.directories, /*include_exact=*/true);

    for (auto it = storage.files.lower_bound(to_prefix); it != storage.files.end() && it->first.starts_with(to_prefix); ++it)
        for (auto & object : it->second.objects)
            object.local_path = it->first;
}

void MetadataStorageFromMemoryTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    std::unique_lock lock(storage.metadata_mutex);

    auto it = storage.files.find(path_from);
    if (it == storage.files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path_from);

    auto record = it->second;
    for (auto & object : record.objects)
        object.local_path = path_to;

    storage.putRecordUnlocked(path_to, std::move(record));
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
