#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageInMemory.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>

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
    extern const int CANNOT_RMDIR;
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
    return getTotalSize(entry->blob_group->objects);
}

Poco::Timestamp MetadataStorageInMemory::getLastModified(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto * entry = findFile(path);
    if (entry)
        return entry->blob_group->last_modified;

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
    /// Match `MetadataStorageFromDisk::getHardlinkCount`: `ref_count` stores the number of *extra* links,
    /// so a regular non-hardlinked file returns 0. Internally we initialise `BlobGroup::ref_count = 1`
    /// to use it as a lifetime counter, so subtract one here to expose the on-disk-equivalent contract.
    return entry->blob_group->ref_count - 1;
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
    return entry->blob_group->inline_data;
}

StoredObjects MetadataStorageInMemory::getStorageObjects(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    auto * entry = findFile(path);
    if (!entry)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
    return entry->blob_group->objects;
}

IMetadataStorage::BlobsToRemove MetadataStorageInMemory::getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    std::lock_guard guard(removed_objects_mutex);

    if (max_count == 0)
        max_count = std::numeric_limits<int64_t>::max();

    BlobsToRemove blobs_to_remove;
    for (const auto & blob : objects_to_remove | std::views::take(max_count))
        blobs_to_remove[blob] = {cluster->getLocalLocation()};

    return blobs_to_remove;
}

int64_t MetadataStorageInMemory::recordAsRemoved(const StoredObjects & blobs)
{
    std::lock_guard guard(removed_objects_mutex);

    int64_t recorded_count = 0;
    for (const auto & removed_blob : blobs)
        recorded_count += objects_to_remove.erase(removed_blob);

    return recorded_count;
}

std::unordered_map<std::string, MetadataStorageInMemory::FileEntry> MetadataStorageInMemory::cloneFiles() const
{
    std::unordered_map<std::string, FileEntry> result;
    result.reserve(files.size());

    /// Preserve sharing: if multiple `FileEntry` instances reference the same `BlobGroup`,
    /// they should still share a (single, freshly cloned) `BlobGroup` in the snapshot.
    std::unordered_map<BlobGroup *, std::shared_ptr<BlobGroup>> clones;

    for (const auto & [path, entry] : files)
    {
        FileEntry copy = entry;
        if (entry.blob_group)
        {
            auto [it, inserted] = clones.try_emplace(entry.blob_group.get(), nullptr);
            if (inserted)
                it->second = std::make_shared<BlobGroup>(*entry.blob_group);
            copy.blob_group = it->second;
        }
        result.emplace(path, std::move(copy));
    }

    return result;
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

        /// Snapshot state before applying, so we can restore on partial failure.
        /// Deep-clone `files` so that in-place mutations of `BlobGroup`
        /// (e.g. `ref_count`, `objects`) by individual operations are also reverted.
        auto files_backup = metadata_storage.cloneFiles();
        auto directories_backup = metadata_storage.directories;

        try
        {
            for (auto & op : operations)
                op();
        }
        catch (...)
        {
            metadata_storage.files = std::move(files_backup);
            metadata_storage.directories = std::move(directories_backup);
            throw;
        }
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
        entry->blob_group->inline_data = data;
    });
}

void MetadataStorageInMemoryTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations.emplace_back([this, path, timestamp]()
    {
        auto * entry = metadata_storage.findFile(path);
        if (entry)
            entry->blob_group->last_modified = timestamp;
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

        auto & blob_group = it->second.blob_group;
        blob_group->ref_count -= 1;

        if (blob_group->ref_count == 0 && should_remove_objects)
        {
            for (const auto & obj : blob_group->objects)
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

        /// Match `DiskLocal::createDirectory` semantics (`mkdir`): reject if a file already
        /// exists at this path, and reject if the parent directory does not exist. Without
        /// these checks metadata can represent impossible states (file and directory with
        /// the same name, or orphan nested directories) that later break `moveDirectory`
        /// or `removeDirectory`.
        std::string without_trailing_slash = normalized.substr(0, normalized.size() - 1);
        if (metadata_storage.findFile(without_trailing_slash) != nullptr)
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot create directory {}: a file with this name already exists", path);

        /// Find parent prefix: for `a/b/`, parent is `a/`; for `a/`, parent is the implicit
        /// disk root (always present). Skip the trailing `/` and look for the previous `/`.
        if (normalized.size() > 1)
        {
            auto parent_end = normalized.rfind('/', normalized.size() - 2);
            if (parent_end != std::string::npos && parent_end > 0)
            {
                std::string parent = normalized.substr(0, parent_end + 1);
                if (!metadata_storage.directories.contains(parent))
                    throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                        "Cannot create directory {}: parent directory does not exist", path);
            }
        }

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

            /// Match `DiskLocal::createDirectories` semantics: refuse to traverse a path component
            /// that already exists as a file. Without this check, `createDirectoryRecursive("a/b")`
            /// would silently insert directory entries even when a file `a` already exists,
            /// fabricating an impossible state where a path is both a file and a directory prefix.
            std::string without_trailing_slash = accumulated.substr(0, accumulated.size() - 1);
            if (metadata_storage.findFile(without_trailing_slash) != nullptr)
                throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                    "Cannot create directory {}: a file exists at intermediate path {}", path, without_trailing_slash);

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

        if (!metadata_storage.directories.contains(normalized))
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory does not exist: {}", path);

        /// Match `DiskLocal::removeDirectory` semantics (`rmdir`): reject if the directory is not empty.
        for (const auto & [file_path, _] : metadata_storage.files)
        {
            if (file_path.starts_with(normalized))
                throw Exception(ErrorCodes::CANNOT_RMDIR, "Directory is not empty: {}", path);
        }

        auto dir_it = metadata_storage.directories.upper_bound(normalized);
        if (dir_it != metadata_storage.directories.end() && dir_it->starts_with(normalized))
            throw Exception(ErrorCodes::CANNOT_RMDIR, "Directory is not empty: {}", path);

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
                auto & blob_group = it->second.blob_group;
                blob_group->ref_count -= 1;

                if (blob_group->ref_count == 0)
                {
                    if (!should_remove_objects || should_remove_objects(fs::relative(it->first, path)))
                    {
                        for (const auto & obj : blob_group->objects)
                            objects_to_remove.push_back(obj);
                    }
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

        /// Match disk-backed semantics (`link(2)`): the destination's parent directory must exist,
        /// and there must be no directory occupying the destination path. Without these checks the
        /// in-memory storage can create unreachable orphan entries (e.g. `non_existing/file`) or
        /// represent a path that is simultaneously a file and a directory prefix.
        auto last_slash = path_to.rfind('/');
        if (last_slash != std::string::npos && last_slash > 0)
        {
            std::string parent = path_to.substr(0, last_slash + 1);
            if (!metadata_storage.directories.contains(parent))
                throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                    "Cannot create hardlink at {}: parent directory does not exist", path_to);
        }
        if (metadata_storage.directories.contains(path_to + "/"))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot create hardlink at {}: a directory already exists at this path", path_to);

        /// Share the blob group between source and destination so all hardlinks observe
        /// the same object list, inline data, and modification time (matching disk inode semantics).
        entry->blob_group->ref_count += 1;
        auto & new_entry = metadata_storage.files[path_to];
        new_entry.blob_group = entry->blob_group;
    });
}

void MetadataStorageInMemoryTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    operations.emplace_back([this, path_from, path_to]()
    {
        auto it_from = metadata_storage.files.find(path_from);
        if (it_from == metadata_storage.files.end())
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path_from);

        /// Match `DiskLocal::moveFile` semantics (`renameNoReplace`): reject if destination exists.
        /// Callers that need overwrite go through `replaceFile`, which routes through unlink semantics.
        if (metadata_storage.files.contains(path_to))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File already exists: {}", path_to);

        metadata_storage.files[path_to] = std::move(it_from->second);
        metadata_storage.files.erase(it_from);
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

        /// Reject moves where destination is the source itself or inside the source subtree
        /// (e.g. `a -> a/b`), matching `DiskLocal::moveDirectory` which would fail via `rename`.
        if (prefix_from == prefix_to || prefix_to.starts_with(prefix_from))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot move directory {} to {}: destination is inside the source subtree",
                path_from, path_to);

        /// Match `DiskLocal::moveDirectory` semantics (`rename`): fail when the source does not
        /// exist as a directory. Without this check, a move of a non-existent source still
        /// inserts `prefix_to` into `directories`, fabricating an empty destination directory.
        if (!metadata_storage.directories.contains(prefix_from))
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                "Source directory does not exist: {}", path_from);

        /// Match `DiskLocal::moveDirectory` semantics (`rename`): reject moves whose destination
        /// already exists as a file or as a directory. Without this check, the loop below would
        /// silently merge source entries into the existing destination tree (overwriting any
        /// conflicting files), which diverges from `MetadataStorageFromDisk` and can hide bugs
        /// that depend on rename atomicity.
        std::string path_to_no_slash = prefix_to.substr(0, prefix_to.size() - 1);
        if (metadata_storage.findFile(path_to_no_slash) != nullptr)
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot move directory {} to {}: a file already exists at destination",
                path_from, path_to);
        if (metadata_storage.directories.contains(prefix_to))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot move directory {} to {}: destination directory already exists",
                path_from, path_to);

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

        /// Self-replace is a no-op. Without this guard the destination lookup would alias
        /// `it_from`, the subsequent erase of `it_to` would invalidate `it_from`, and the move
        /// from `it_from->second` would dereference an invalidated iterator (undefined behavior).
        if (path_from == path_to)
            return;

        auto it_to = metadata_storage.files.find(path_to);
        if (it_to != metadata_storage.files.end())
        {
            auto & blob_group = it_to->second.blob_group;
            blob_group->ref_count -= 1;
            if (blob_group->ref_count == 0)
            {
                for (const auto & obj : blob_group->objects)
                    objects_to_remove.push_back(obj);
            }
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
        /// Match `MetadataStorageFromDisk` semantics (see `gtest_metadata_local_disk`
        /// `TestNonExistingObjectsInTransaction`): refuse to create a file when its parent
        /// directory does not exist. The disk-backed implementation fails when opening the
        /// underlying metadata file; without this check, in-memory transactions can fabricate
        /// orphan paths like `non-existing/file.txt` whose parent was never created.
        auto last_slash = path.rfind('/');
        if (last_slash != std::string::npos && last_slash > 0)
        {
            std::string parent = path.substr(0, last_slash + 1);
            if (!metadata_storage.directories.contains(parent))
                throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                    "Cannot create file {}: parent directory does not exist", path);
        }

        /// Reject creating a file at a path that already exists as a directory. Disk-backed
        /// metadata storage rejects this naturally because the underlying filesystem cannot
        /// have a file and a directory with the same name; without this check, in-memory
        /// metadata could represent that impossible state and confuse subsequent operations.
        if (metadata_storage.directories.contains(path + "/"))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot create file {}: a directory already exists at this path", path);

        auto it = metadata_storage.files.find(path);
        if (it != metadata_storage.files.end())
        {
            /// If replacing existing file, handle old blob group
            auto & old_group = it->second.blob_group;
            old_group->ref_count -= 1;
            if (old_group->ref_count == 0)
            {
                for (const auto & obj : old_group->objects)
                    objects_to_remove.push_back(obj);
            }
        }

        auto & entry = metadata_storage.files[path];
        entry.blob_group = std::make_shared<MetadataStorageInMemory::BlobGroup>();
        entry.blob_group->objects = objects;
        entry.blob_group->last_modified = Poco::Timestamp();
    });
}

void MetadataStorageInMemoryTransaction::addBlobToMetadata(const std::string & path, const StoredObject & object)
{
    operations.emplace_back([this, path, object]()
    {
        auto * entry = metadata_storage.findFile(path);
        if (!entry)
        {
            /// Reject implicit file creation at a path that already exists as a directory,
            /// matching the same constraint enforced in `createMetadataFile`.
            if (metadata_storage.directories.contains(path + "/"))
                throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                    "Cannot create file {}: a directory already exists at this path", path);

            /// Create new file entry if it doesn't exist
            metadata_storage.files[path] = MetadataStorageInMemory::FileEntry{};
            entry = &metadata_storage.files[path];
        }
        entry->blob_group->objects.push_back(object);
        entry->blob_group->last_modified = Poco::Timestamp();
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
        for (const auto & obj : entry->blob_group->objects)
        {
            if (accumulated_size >= target_size)
            {
                objects_to_remove.push_back(obj);
                continue;
            }

            new_objects.push_back(obj);
            accumulated_size += obj.bytes_size;
        }
        entry->blob_group->objects = std::move(new_objects);
        entry->blob_group->last_modified = Poco::Timestamp();
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
