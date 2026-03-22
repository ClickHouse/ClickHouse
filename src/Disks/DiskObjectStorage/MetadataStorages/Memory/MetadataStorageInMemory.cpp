#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageInMemory.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <filesystem>
#include <ranges>
#include <shared_mutex>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MetadataStorageInMemory::MetadataStorageInMemory(
    std::string compatible_key_prefix_,
    ObjectStorageKeyGeneratorPtr key_generator_)
    : compatible_key_prefix(std::move(compatible_key_prefix_))
    , key_generator(std::move(key_generator_))
    , root_path("/")
{
}

MetadataTransactionPtr MetadataStorageInMemory::createTransaction()
{
    return std::make_shared<MetadataStorageInMemoryTransaction>(*this);
}

const std::string & MetadataStorageInMemory::getPath() const
{
    return root_path;
}

MetadataStorageInMemory::FileEntry * MetadataStorageInMemory::findFile(const std::string & path) const
{
    auto it = files.find(path);
    if (it == files.end())
        return nullptr;
    return &it->second;
}

bool MetadataStorageInMemory::existsFile(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return findFile(path) != nullptr;
}

bool MetadataStorageInMemory::existsDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    std::string normalized = path;
    if (!normalized.empty() && normalized.back() != '/')
        normalized += '/';
    return directories.contains(normalized);
}

bool MetadataStorageInMemory::existsFileOrDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    if (findFile(path) != nullptr)
        return true;

    std::string normalized = path;
    if (!normalized.empty() && normalized.back() != '/')
        normalized += '/';
    return directories.contains(normalized);
}

uint64_t MetadataStorageInMemory::getFileSize(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto * entry = findFile(path);
    if (!entry)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
    return getTotalSize(entry->objects);
}

Poco::Timestamp MetadataStorageInMemory::getLastModified(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto * entry = findFile(path);
    if (entry)
        return entry->last_modified;

    /// Check if it's a directory
    std::string normalized = path;
    if (!normalized.empty() && normalized.back() != '/')
        normalized += '/';
    if (directories.contains(normalized))
        return {};

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Path does not exist: {}", path);
}

std::vector<std::string> MetadataStorageInMemory::listDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);

    std::string prefix = path;
    if (!prefix.empty() && prefix.back() != '/')
        prefix += '/';

    std::set<std::string> result;

    /// Find direct children among files
    for (const auto & [file_path, _] : files)
    {
        if (file_path.starts_with(prefix) && file_path.size() > prefix.size())
        {
            auto remainder = std::string_view(file_path).substr(prefix.size());
            auto slash_pos = remainder.find('/');
            if (slash_pos == std::string_view::npos)
                result.insert(std::string(remainder));
        }
    }

    /// Find direct children among directories
    for (const auto & dir_path : directories)
    {
        if (dir_path.starts_with(prefix) && dir_path.size() > prefix.size())
        {
            auto remainder = std::string_view(dir_path).substr(prefix.size());
            /// remainder ends with '/' since directory paths are normalized
            auto slash_pos = remainder.find('/');
            if (slash_pos == remainder.size() - 1)
                result.insert(std::string(remainder.substr(0, slash_pos)));
        }
    }

    return {result.begin(), result.end()};
}

DirectoryIteratorPtr MetadataStorageInMemory::iterateDirectory(const std::string & path) const
{
    auto children = listDirectory(path);
    std::string prefix = path;
    if (!prefix.empty() && prefix.back() != '/')
        prefix += '/';

    std::vector<fs::path> paths;
    paths.reserve(children.size());
    for (auto & child : children)
        paths.emplace_back(prefix + child);

    return std::make_unique<StaticDirectoryIterator>(std::move(paths));
}

uint32_t MetadataStorageInMemory::getHardlinkCount(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto * entry = findFile(path);
    if (!entry)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
    return entry->ref_count;
}

std::string MetadataStorageInMemory::readFileToString(const std::string & /* path */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "readFileToString is not supported by in-memory metadata storage");
}

std::string MetadataStorageInMemory::readInlineDataToString(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto * entry = findFile(path);
    if (!entry)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
    return entry->inline_data;
}

StoredObjects MetadataStorageInMemory::getStorageObjects(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto * entry = findFile(path);
    if (!entry)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
    return entry->objects;
}

/// ==================== Transaction ====================

MetadataStorageInMemoryTransaction::MetadataStorageInMemoryTransaction(MetadataStorageInMemory & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

void MetadataStorageInMemoryTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);
        for (auto & op : operations)
            op();
    }

    {
        std::lock_guard guard(metadata_storage.removed_objects_mutex);
        metadata_storage.objects_to_remove.insert_range(objects_to_remove);
    }
}

TransactionCommitOutcomeVariant MetadataStorageInMemoryTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "In-memory metadata storage supports only tryCommit without options");

    commit(NoCommitOptions{});
    return true;
}

void MetadataStorageInMemoryTransaction::writeStringToFile(const std::string & /* path */, const std::string & /* data */)
{
    throwNotImplemented();
}

void MetadataStorageInMemoryTransaction::writeInlineDataToFile(const std::string & path, const std::string & data)
{
    operations.emplace_back([this, path, data]()
    {
        auto * entry = metadata_storage.findFile(path);
        if (!entry)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
        entry->inline_data = data;
    });
}

void MetadataStorageInMemoryTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations.emplace_back([this, path, timestamp]()
    {
        auto * entry = metadata_storage.findFile(path);
        if (entry)
            entry->last_modified = timestamp;
    });
}

void MetadataStorageInMemoryTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    operations.emplace_back([this, path, if_exists, should_remove_objects]()
    {
        auto it = metadata_storage.files.find(path);
        if (it == metadata_storage.files.end())
        {
            if (if_exists)
                return;
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
        }

        if (it->second.ref_count > 0)
        {
            /// Other hardlinks still reference this data, just decrement.
            it->second.ref_count -= 1;
            return;
        }

        if (should_remove_objects)
        {
            for (const auto & obj : it->second.objects)
                objects_to_remove.push_back(obj);
        }

        metadata_storage.files.erase(it);
    });
}

void MetadataStorageInMemoryTransaction::createDirectory(const std::string & path)
{
    operations.emplace_back([this, path]()
    {
        std::string normalized = path;
        if (!normalized.empty() && normalized.back() != '/')
            normalized += '/';
        metadata_storage.directories.insert(normalized);
    });
}

void MetadataStorageInMemoryTransaction::createDirectoryRecursive(const std::string & path)
{
    operations.emplace_back([this, path]()
    {
        fs::path p(path);
        std::string accumulated;
        for (const auto & component : p)
        {
            if (component == "/")
            {
                accumulated = "/";
                continue;
            }
            accumulated += component.string() + "/";
            metadata_storage.directories.insert(accumulated);
        }
    });
}

void MetadataStorageInMemoryTransaction::removeDirectory(const std::string & path)
{
    operations.emplace_back([this, path]()
    {
        std::string normalized = path;
        if (!normalized.empty() && normalized.back() != '/')
            normalized += '/';
        metadata_storage.directories.erase(normalized);
    });
}

void MetadataStorageInMemoryTransaction::removeRecursive(
    const std::string & path,
    const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    operations.emplace_back([this, path, should_remove_objects]()
    {
        std::string prefix = path;
        if (!prefix.empty() && prefix.back() != '/')
            prefix += '/';

        /// Remove files with this prefix
        for (auto it = metadata_storage.files.begin(); it != metadata_storage.files.end();)
        {
            if (it->first.starts_with(prefix) || it->first == path)
            {
                if (should_remove_objects(it->first))
                {
                    for (const auto & obj : it->second.objects)
                        objects_to_remove.push_back(obj);
                }
                it = metadata_storage.files.erase(it);
            }
            else
            {
                ++it;
            }
        }

        /// Remove directories with this prefix
        auto dir_it = metadata_storage.directories.lower_bound(prefix);
        while (dir_it != metadata_storage.directories.end() && dir_it->starts_with(prefix))
            dir_it = metadata_storage.directories.erase(dir_it);

        /// Also remove the directory itself
        metadata_storage.directories.erase(prefix);
    });
}

void MetadataStorageInMemoryTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    operations.emplace_back([this, path_from, path_to]()
    {
        auto * entry = metadata_storage.findFile(path_from);
        if (!entry)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path_from);

        if (metadata_storage.findFile(path_to))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File already exists: {}", path_to);

        entry->ref_count += 1;
        metadata_storage.files[path_to] = *entry;
    });
}

void MetadataStorageInMemoryTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    operations.emplace_back([this, path_from, path_to]()
    {
        auto it = metadata_storage.files.find(path_from);
        if (it == metadata_storage.files.end())
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path_from);

        metadata_storage.files[path_to] = std::move(it->second);
        metadata_storage.files.erase(it);
    });
}

void MetadataStorageInMemoryTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    operations.emplace_back([this, path_from, path_to]()
    {
        std::string prefix_from = path_from;
        if (!prefix_from.empty() && prefix_from.back() != '/')
            prefix_from += '/';

        std::string prefix_to = path_to;
        if (!prefix_to.empty() && prefix_to.back() != '/')
            prefix_to += '/';

        /// Move files
        std::vector<std::pair<std::string, MetadataStorageInMemory::FileEntry>> to_move;
        for (auto it = metadata_storage.files.begin(); it != metadata_storage.files.end();)
        {
            if (it->first.starts_with(prefix_from))
            {
                std::string new_path = prefix_to + it->first.substr(prefix_from.size());
                to_move.emplace_back(new_path, std::move(it->second));
                it = metadata_storage.files.erase(it);
            }
            else
            {
                ++it;
            }
        }
        for (auto & [new_path, entry] : to_move)
            metadata_storage.files[new_path] = std::move(entry);

        /// Move directories
        std::vector<std::string> dirs_to_add;
        auto dir_it = metadata_storage.directories.lower_bound(prefix_from);
        while (dir_it != metadata_storage.directories.end() && dir_it->starts_with(prefix_from))
        {
            dirs_to_add.push_back(prefix_to + dir_it->substr(prefix_from.size()));
            dir_it = metadata_storage.directories.erase(dir_it);
        }
        for (auto & d : dirs_to_add)
            metadata_storage.directories.insert(std::move(d));

        /// Replace directory entry itself
        metadata_storage.directories.erase(prefix_from);
        metadata_storage.directories.insert(prefix_to);
    });
}

void MetadataStorageInMemoryTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    operations.emplace_back([this, path_from, path_to]()
    {
        auto it_from = metadata_storage.files.find(path_from);
        if (it_from == metadata_storage.files.end())
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path_from);

        auto it_to = metadata_storage.files.find(path_to);
        if (it_to != metadata_storage.files.end())
        {
            for (const auto & obj : it_to->second.objects)
                objects_to_remove.push_back(obj);
            metadata_storage.files.erase(it_to);
        }

        metadata_storage.files[path_to] = std::move(it_from->second);
        metadata_storage.files.erase(it_from);
    });
}

void MetadataStorageInMemoryTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    operations.emplace_back([this, path, objects]()
    {
        auto & entry = metadata_storage.files[path];
        /// If replacing existing file, schedule old objects for removal
        if (!entry.objects.empty())
        {
            for (const auto & obj : entry.objects)
                objects_to_remove.push_back(obj);
        }
        entry.objects = objects;
        entry.ref_count = 0;
        entry.last_modified = Poco::Timestamp();
    });
}

void MetadataStorageInMemoryTransaction::addBlobToMetadata(const std::string & path, const StoredObject & object)
{
    operations.emplace_back([this, path, object]()
    {
        auto * entry = metadata_storage.findFile(path);
        if (!entry)
        {
            /// Create new file entry if it doesn't exist
            metadata_storage.files[path] = MetadataStorageInMemory::FileEntry{};
            entry = &metadata_storage.files[path];
        }
        entry->objects.push_back(object);
        entry->last_modified = Poco::Timestamp();
    });
}

void MetadataStorageInMemoryTransaction::truncateFile(const std::string & path, size_t target_size)
{
    operations.emplace_back([this, path, target_size]()
    {
        auto * entry = metadata_storage.findFile(path);
        if (!entry)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);

        size_t accumulated_size = 0;
        StoredObjects new_objects;
        for (const auto & obj : entry->objects)
        {
            if (accumulated_size >= target_size)
            {
                objects_to_remove.push_back(obj);
                continue;
            }

            new_objects.push_back(obj);
            accumulated_size += obj.bytes_size;
        }
        entry->objects = std::move(new_objects);
        entry->last_modified = Poco::Timestamp();
    });
}

ObjectStorageKey MetadataStorageInMemoryTransaction::generateObjectKeyForPath(const std::string & /* path */)
{
    return metadata_storage.key_generator->generate();
}

StoredObjects MetadataStorageInMemoryTransaction::getSubmittedForRemovalBlobs()
{
    return objects_to_remove;
}

}
