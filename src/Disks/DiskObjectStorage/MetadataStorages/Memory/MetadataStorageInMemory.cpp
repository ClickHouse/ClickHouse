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
    /// The disk root is implicit and not stored in `directories` (entries use relative paths
    /// like `a/`, `a/b/`). Other backends (`MetadataStorageFromDisk`, plain object storage)
    /// report the root as existing, so do the same here.
    if (path == "/" || path.empty())
        return true;
    std::string normalized = path;
    if (normalized.back() != '/')
        normalized += '/';
    return directories.contains(normalized);
}

bool MetadataStorageInMemory::existsFileOrDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    if (path == "/" || path.empty())
        return true;
    if (findFile(path) != nullptr)
        return true;

    std::string normalized = path;
    if (normalized.back() != '/')
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

    /// Root is always a directory, regardless of whether anything was created under it.
    if (path == "/" || path.empty())
        return {};

    /// Check if it's a directory
    std::string normalized = path;
    if (normalized.back() != '/')
        normalized += '/';
    if (directories.contains(normalized))
        return {};

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Path does not exist: {}", path);
}

std::vector<std::string> MetadataStorageInMemory::listDirectory(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);

    /// Stored entries use relative paths (e.g. `a/`, `a/b/file`). For the disk root (`"/"` or
    /// empty), treat the prefix as empty so all top-level entries are matched as direct children.
    std::string prefix;
    if (path != "/" && !path.empty())
    {
        prefix = path;
        if (prefix.back() != '/')
            prefix += '/';
    }

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

    /// Paths are stored relative to the disk root (e.g. `a/b/file` rather than `/a/b/file`),
    /// and lookup methods like `findFile` match the stored form. Keep the iterator output
    /// in the same form so its values can be passed back into other methods of this storage.
    std::string prefix;
    if (path != "/" && !path.empty())
    {
        prefix = path;
        if (prefix.back() != '/')
            prefix += '/';
    }

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

/// ==================== Transaction ====================

MetadataStorageInMemoryTransaction::MetadataStorageInMemoryTransaction(MetadataStorageInMemory & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

void MetadataStorageInMemoryTransaction::recordFileBefore(const std::string & path)
{
    if (files_undo.contains(path))
        return;
    auto it = metadata_storage.files.find(path);
    if (it == metadata_storage.files.end())
        files_undo.emplace(path, std::nullopt);
    else
        files_undo.emplace(path, it->second);
}

void MetadataStorageInMemoryTransaction::recordBlobGroupBefore(const std::shared_ptr<MetadataStorageInMemory::BlobGroup> & group)
{
    if (!group)
        return;
    auto [it, inserted] = blob_group_undo.try_emplace(group.get());
    if (inserted)
        it->second = BlobGroupSnapshot{group, *group};
}

void MetadataStorageInMemoryTransaction::recordDirInsert(const std::string & dir)
{
    /// Net effect tracking: if we previously erased this dir, the insert cancels it
    /// (state returns to pre-transaction); otherwise note it so rollback can erase.
    if (dirs_erased.erase(dir) > 0)
        return;
    if (!metadata_storage.directories.contains(dir))
        dirs_inserted.insert(dir);
}

void MetadataStorageInMemoryTransaction::recordDirErase(const std::string & dir)
{
    if (dirs_inserted.erase(dir) > 0)
        return;
    if (metadata_storage.directories.contains(dir))
        dirs_erased.insert(dir);
}

void MetadataStorageInMemoryTransaction::rollback()
{
    /// Restore in-place `BlobGroup` content first, so subsequent file restoration
    /// re-attaches entries to groups whose `ref_count`/`objects` are already correct.
    for (auto & [ptr, snap] : blob_group_undo)
        *snap.alive = std::move(snap.snapshot);

    for (auto & [path, opt_entry] : files_undo)
    {
        if (opt_entry)
            metadata_storage.files[path] = std::move(*opt_entry);
        else
            metadata_storage.files.erase(path);
    }

    for (const auto & dir : dirs_inserted)
        metadata_storage.directories.erase(dir);
    for (const auto & dir : dirs_erased)
        metadata_storage.directories.insert(dir);
}

void MetadataStorageInMemoryTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);

        /// Operation-level rollback: each operation records the pre-mutation state of
        /// every entry it touches into `files_undo` / `blob_group_undo` / `dirs_inserted`
        /// / `dirs_erased`. On a partial-failure exception, `rollback` replays only those
        /// touched entries, keeping commit cost proportional to changed entries instead
        /// of the whole `files` map (which can be very large for `borrow_from_cache`).
        try
        {
            for (auto & op : operations)
                op();
        }
        catch (...)
        {
            rollback();
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
        recordBlobGroupBefore(entry->blob_group);
        entry->blob_group->inline_data = data;
    });
}

void MetadataStorageInMemoryTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations.emplace_back([this, path, timestamp]()
    {
        auto * entry = metadata_storage.findFile(path);
        if (entry)
        {
            recordBlobGroupBefore(entry->blob_group);
            entry->blob_group->last_modified = timestamp;
        }
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

        recordFileBefore(path);
        recordBlobGroupBefore(it->second.blob_group);

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

        recordDirInsert(normalized);
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

            recordDirInsert(accumulated);
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

        recordDirErase(normalized);
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
                recordFileBefore(it->first);
                recordBlobGroupBefore(it->second.blob_group);

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
        {
            recordDirErase(*dir_it);
            dir_it = metadata_storage.directories.erase(dir_it);
        }

        /// Also remove the directory itself
        recordDirErase(prefix);
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

        recordFileBefore(path_to);
        recordBlobGroupBefore(entry->blob_group);

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

        /// Match disk-backed semantics (`rename(2)`): the destination's parent directory must exist,
        /// and there must be no directory occupying the destination path. Without these checks the
        /// in-memory storage could create orphan entries (e.g. `non_existing/file`) or represent a
        /// path that is simultaneously a file and a directory prefix.
        auto last_slash = path_to.rfind('/');
        if (last_slash != std::string::npos && last_slash > 0)
        {
            std::string parent = path_to.substr(0, last_slash + 1);
            if (!metadata_storage.directories.contains(parent))
                throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                    "Cannot move file to {}: parent directory does not exist", path_to);
        }
        if (metadata_storage.directories.contains(path_to + "/"))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot move file to {}: a directory already exists at this path", path_to);

        recordFileBefore(path_from);
        recordFileBefore(path_to);

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

        /// Match `DiskLocal::moveDirectory` semantics (`rename`): the destination's parent directory
        /// must exist. Without this check, `moveDirectory("a", "x/y")` would succeed even when `x/`
        /// has never been created, producing an invalid topology (the moved subtree becomes
        /// unreachable from a recursive listing rooted at `/`).
        if (prefix_to.size() > 1)
        {
            auto parent_end = prefix_to.rfind('/', prefix_to.size() - 2);
            if (parent_end != std::string::npos && parent_end > 0)
            {
                std::string parent = prefix_to.substr(0, parent_end + 1);
                if (!metadata_storage.directories.contains(parent))
                    throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                        "Cannot move directory {} to {}: parent of destination does not exist",
                        path_from, path_to);
            }
        }

        /// Move files
        std::vector<std::pair<std::string, MetadataStorageInMemory::FileEntry>> to_move;
        for (auto it = metadata_storage.files.begin(); it != metadata_storage.files.end();)
        {
            if (it->first.starts_with(prefix_from))
            {
                std::string new_path = prefix_to + it->first.substr(prefix_from.size());
                recordFileBefore(it->first);
                recordFileBefore(new_path);
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
            recordDirErase(*dir_it);
            dir_it = metadata_storage.directories.erase(dir_it);
        }
        for (auto & d : dirs_to_add)
        {
            recordDirInsert(d);
            metadata_storage.directories.insert(std::move(d));
        }

        /// Replace directory entry itself
        recordDirErase(prefix_from);
        metadata_storage.directories.erase(prefix_from);
        recordDirInsert(prefix_to);
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

        /// Match disk-backed semantics (`rename(2)`): the destination's parent directory must exist,
        /// and there must be no directory occupying the destination path. Without these checks the
        /// in-memory storage could create orphan entries or a path that is simultaneously a file
        /// and a directory prefix.
        auto last_slash = path_to.rfind('/');
        if (last_slash != std::string::npos && last_slash > 0)
        {
            std::string parent = path_to.substr(0, last_slash + 1);
            if (!metadata_storage.directories.contains(parent))
                throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                    "Cannot replace file at {}: parent directory does not exist", path_to);
        }
        if (metadata_storage.directories.contains(path_to + "/"))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot replace file at {}: a directory already exists at this path", path_to);

        recordFileBefore(path_from);
        recordFileBefore(path_to);

        auto it_to = metadata_storage.files.find(path_to);
        if (it_to != metadata_storage.files.end())
        {
            recordBlobGroupBefore(it_to->second.blob_group);
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
            /// Rewriting an existing file must preserve hardlink semantics:
            /// disk-backed metadata storage shares one inode across hardlinks, so updating
            /// metadata via any link is observable from every other link (see `TestHardlinkRewrite`
            /// in `gtest_metadata_local_disk.cpp`). Update the shared `BlobGroup` in place
            /// instead of replacing the pointer for just this path.
            recordBlobGroupBefore(it->second.blob_group);
            auto & blob_group = it->second.blob_group;
            for (const auto & obj : blob_group->objects)
                objects_to_remove.push_back(obj);
            blob_group->objects = objects;
            blob_group->inline_data.clear();
            blob_group->last_modified = Poco::Timestamp();
        }
        else
        {
            recordFileBefore(path);
            auto & entry = metadata_storage.files[path];
            entry.blob_group->objects = objects;
            entry.blob_group->last_modified = Poco::Timestamp();
        }
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

            /// Match `MetadataStorageFromDisk::AddBlobOperation` semantics: implicit creation goes
            /// through `WriteFileOperation`, which fails if the parent directory is missing. Without
            /// this check, a transaction could create orphan paths (e.g. `non_existing/file`) that
            /// disk-backed metadata would reject.
            auto last_slash = path.rfind('/');
            if (last_slash != std::string::npos && last_slash > 0)
            {
                std::string parent = path.substr(0, last_slash + 1);
                if (!metadata_storage.directories.contains(parent))
                    throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                        "Cannot create file {}: parent directory does not exist", path);
            }

            /// Create new file entry if it doesn't exist
            recordFileBefore(path);
            metadata_storage.files[path] = MetadataStorageInMemory::FileEntry{};
            entry = &metadata_storage.files[path];
        }
        recordBlobGroupBefore(entry->blob_group);
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

        recordBlobGroupBefore(entry->blob_group);

        auto & objects = entry->blob_group->objects;
        size_t current_size = 0;
        for (const auto & obj : objects)
            current_size += obj.bytes_size;

        while (current_size > target_size && !objects.empty())
        {
            current_size -= objects.back().bytes_size;
            objects_to_remove.push_back(objects.back());
            objects.pop_back();
        }

        if (current_size != target_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} can't be truncated to size {}", path, target_size);

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
