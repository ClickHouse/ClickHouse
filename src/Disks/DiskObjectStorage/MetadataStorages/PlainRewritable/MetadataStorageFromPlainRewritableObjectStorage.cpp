#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PrefixPath.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <IO/S3Common.h>
#include <IO/SharedThreadPools.h>
#include <Poco/Timestamp.h>
#include <Common/CurrentMetrics.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <Common/threadPoolCallbackRunner.h>

#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/storage_exception.hpp>
#endif

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableLegacyLayoutDiskCount;
    extern const Event DiskPlainRewritableOrphanedObjectsRemoved;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char plain_rewritable_object_storage_azure_not_found_on_init[];
}

namespace
{

fs::path normalizeDirectoryPath(const fs::path & path)
{
    return path / "";
}

/// The names of `PlainRewritableLayout::generateRemovedName` are used for the leftovers of a removal that has
/// been committed but not finished, and are deleted on the next load if they are marked as such, so nobody may
/// introduce a new top-level entry with such a name from now on. An entry of this shape that already exists on
/// a disk is not affected: without a marker it is ordinary data (see `PlainRewritableLayout::REMOVED_NAME_PREFIX`),
/// created by a version that reserved nothing, so it stays usable - it can be rewritten and written into.
/// A marked entry is never loaded, so it never exists here, and the check rejects it too.
void checkNotReservedPath(const std::string & path, const UncommittedState & uncommitted_state)
{
    const auto removed_name = PlainRewritableLayout::getRemovedNameOfLocalPath(path);
    if (!removed_name || uncommitted_state.existsFileOrDirectory(*removed_name))
        return;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Cannot create '{}' on a plain_rewritable disk: the names of '{}' followed by {} lowercase Latin letters are reserved",
        path,
        PlainRewritableLayout::REMOVED_NAME_PREFIX,
        PlainRewritableLayout::REMOVED_NAME_RANDOM_PART_SIZE);
}

std::optional<std::string> getBlobKeyIfExists(const FsSnapshot & snapshot, const NormalizedPath & path)
{
    const auto directory = snapshot.getDirectoryRemoteInfo(path.parent_path());
    if (!directory)
        return std::nullopt;

    const auto it = directory->files.find(path.filename());
    if (it == directory->files.end())
        return std::nullopt;

    return getBlobKey(*directory, path.filename(), it->second);
}

/// The pages of one listing can only be fetched one after another, so enumerating a disk that holds
/// hundreds of thousands of objects takes hundreds of sequential requests, and no amount of threads
/// makes it faster. Directory names in this layout are random strings produced by `getRandomASCIIString`,
/// so grouping them by their first characters splits the key space into nearly equal parts that can be
/// listed in parallel. This alphabet must stay in sync with `getRandomASCIIString`; names outside of it
/// (written by another implementation) are still enumerated, by the two boundary shards below.
constexpr std::string_view DIRECTORY_NAME_ALPHABET = "abcdefghijklmnopqrstuvwxyz";

/// The point of the sharded listing is to replace many sequential pages with a few parallel ones,
/// so it only pays off once a plain listing would need many pages. Until then a plain listing costs
/// a single request while a sharded one costs one request per shard.
constexpr size_t MIN_DIRECTORIES_TO_LIST_IN_PARALLEL = 4096;

/// A listing returns at most `list_object_keys_size` (1000 by default) objects per request, so this
/// is how many directories a single shard should cover for its listing to fit in about one page.
/// It only controls how the work is split: a shard that turns out bigger is simply paginated.
constexpr size_t DIRECTORIES_PER_LISTING_SHARD = 64;

/// A longer prefix means more shards; 26^3 shards would be far more requests than pages to fetch.
constexpr size_t MAX_SHARD_PREFIX_LENGTH = 3;

/// A `__meta/{directory}/prefix.path` object, which is what makes a directory of the disk exist.
struct DirectoryObject
{
    std::string object_path;
    std::string remote_path;
    std::optional<ObjectMetadata> metadata;
};

/// The outcome of loading one directory. `loaded` stays false for a directory that disappeared while
/// it was being read, which is not an error: it is simply absent from the resulting layout.
struct DirectoryLoadResult
{
    bool loaded = false;
    /// The directory is a leftover of a removal that was committed but not finished: it is not put into
    /// the layout, its objects are deleted instead.
    bool is_orphaned = false;
    std::string object_path;
    std::string local_path;
    DirectoryRemoteInfo info;
};

/// Split `names` into groups by their common prefix, so that listing every returned prefix in turn
/// enumerates exactly the same objects as listing the whole directory, but in `target_count` requests
/// that can run in parallel.
std::vector<std::string> makeListingShards(const std::vector<std::string> & names, size_t target_count)
{
    std::unordered_set<std::string> prefixes;
    for (size_t length = 1; length <= MAX_SHARD_PREFIX_LENGTH; ++length)
    {
        prefixes.clear();
        for (const auto & name : names)
            prefixes.emplace(name, 0, length);
        if (prefixes.size() >= target_count)
            break;
    }
    return {prefixes.begin(), prefixes.end()};
}

}

void MetadataStorageFromPlainRewritableObjectStorage::load(bool is_initial_load, bool do_not_load_unchanged_directories)
{
    ThreadPool & pool = getIOThreadPool().get();

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.useForSmallRemoteRead(1024);  /// These files are small.

    LOG_DEBUG(log, "Loading metadata");

    /// This method can do both initial loading and incremental refresh of the metadata.
    ///
    /// We will list directories under __meta and compare it with the current list in memory.
    /// Some directories may be new and some no longer exist in the storage.
    /// We want to update the state in memory without holding a lock,
    /// and we can do it while allowing certain race-conditions.

    /// So, we obtain a list, then apply changes by:
    /// 1. Deleting every directory in memory that no longer present in the storage;
    ///    This works correctly under the assumption that if a directory with a certain name was deleted it cannot appear again.
    ///    And this assumption is satisfied, because every name is a unique random value.
    /// 2. Checking the value of `prefix.path` for every new directory and adding it to the state in memory.
    ///    There is (?) a race condition, leading to the possibility to add a directory that was just deleted.
    ///    This race condition can be ignored for MergeTree tables.
    /// 3. Checking if the value of `prefix.path` changed for any already existing directory
    ///    and apply the corresponding rename.

    bool has_metadata = object_storage->existsOrHasAnyChild(layout->constructMetadataDirectoryKey());

    std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout;
    remote_layout[""] = DirectoryRemoteInfo{PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, "fake_etag", 0, {}};

    if (is_initial_load)
    {
        /// Use iteration to determine if the disk contains data.
        /// LocalObjectStorage creates an empty top-level directory even when no data is stored,
        /// unlike blob storage, which has no concept of directories, therefore existsOrHasAnyChild
        /// is not applicable.
        auto common_key_prefix = fs::path(object_storage->getCommonKeyPrefix()) / "";
        bool has_data = object_storage->isRemote() ? object_storage->existsOrHasAnyChild(common_key_prefix) : object_storage->iterate(common_key_prefix, 0, /*with_tags=*/ false, std::nullopt)->isValid();
        /// No metadata directory: legacy layout is likely in use.
        if (has_data && !has_metadata)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableLegacyLayoutDiskCount, 1);
            LOG_WARNING(log, "Legacy layout is likely used for disk '{}'", object_storage->getCommonKeyPrefix());
        }

        if (!has_data && !has_metadata)
        {
            LOG_DEBUG(log, "Loaded metadata (empty)");
            fs.applyLayout(std::move(remote_layout));
            return;
        }
    }

    const auto read_snapshot = fs.takeReadOnlySnapshot();

    /// A removal that has been committed but not finished leaves its objects under a reserved name
    /// (see `PlainRewritableLayout::REMOVED_NAME_PREFIX`) and a marker object for that name. Only a name that
    /// has a marker is a leftover of a removal: a name of the same shape without one is ordinary data, possibly
    /// created by a version that reserved nothing, so it is loaded and never touched.
    ///
    /// The marked objects are never loaded. They are deleted during the initial load only: a subsequent load may
    /// run concurrently with the `finalize` of a transaction that has just committed such a removal, and that
    /// `finalize` deletes them itself.
    ///
    /// A marker can also say that its removal is pending: the subtree was being moved under the reserved name and
    /// the move may be incomplete (see `PlainRewritableLayout::PENDING_TOMBSTONE_PREFIX`). Such a removal is
    /// rolled back during the initial load: the directories under its name are loaded under their original paths,
    /// and the writable load rewrites their `prefix.path` back. Nothing of it is deleted. A subsequent load skips
    /// them, because the removal may be running in this very process.
    ///
    /// A marker can also say that a replacement of a file is pending (see `PlainRewritableLayout::PENDING_REPLACE_TOMBSTONE_PREFIX`):
    /// the backup of the target it marks may be the only copy of the target. The initial load copies a complete
    /// backup back over the target before any listing, and then reclaims the backup like a committed removal.
    ///
    /// A read-only disk changes nothing on the object storage, but it shows the last committed state just the same:
    /// a pending removal is loaded under its original paths, and the target of a pending replacement is read from its
    /// complete backup. It does so on every load, not only on the initial one, because it never runs an operation
    /// itself, so every unfinished operation it sees belongs to another process.
    ///
    /// A marker whose content is torn (see `PlainRewritableLayout::parseTombstoneMarkerContent`) tells neither whether
    /// its removal was committed nor where its subtree came from. If anything on the disk still has its name, the load
    /// fails rather than guess: either guess may lose data. Otherwise the process died while writing the first marker
    /// of an operation that had not touched anything yet, and the initial load just deletes the marker.
    ///
    /// The value is the original path of the subtree for a pending removal, and empty for a committed one.
    std::unordered_map<std::string, std::optional<std::string>> tombstones;
    std::unordered_set<std::string> malformed_markers;
    std::unordered_map<std::string, PlainRewritableLayout::PendingReplace> pending_replaces;
    /// The backups of pending replacements that can be neither restored nor reclaimed: they are not loaded and not deleted.
    std::unordered_set<std::string> kept_backups;
    for (auto iterator = object_storage->iterate(layout->constructTombstoneDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
    {
        const auto marker = iterator->current();
        auto removed_name = layout->parseTombstoneMarkerKey(marker->getPath());
        if (!removed_name)
            continue;

        std::string content;
        try
        {
            auto read_buf = object_storage->readObject(StoredObject(marker->getPath()), settings);
            readStringUntilEOF(content, *read_buf);
        }
#if USE_AWS_S3
        catch (const S3Exception & e)
        {
            /// The removal has just been finished or rolled back by a concurrent transaction.
            if (e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
                continue;
            throw;
        }
#endif
#if USE_AZURE_BLOB_STORAGE
        catch (const Azure::Storage::StorageException & e)
        {
            if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
                continue;
            throw;
        }
#endif
        catch (const ErrnoException & e)
        {
            /// The same for the local object storage.
            if (e.getErrno() == ENOENT)
                continue;
            throw;
        }
        catch (...)
        {
            throw;
        }

        auto parsed = PlainRewritableLayout::parseTombstoneMarkerContent(content, removed_name.value());
        switch (parsed.kind)
        {
            case PlainRewritableLayout::TombstoneMarker::Kind::Committed:
                tombstones.emplace(std::move(removed_name.value()), std::nullopt);
                break;
            case PlainRewritableLayout::TombstoneMarker::Kind::PendingRemoval:
                tombstones.emplace(std::move(removed_name.value()), std::move(parsed.pending_original_path));
                break;
            case PlainRewritableLayout::TombstoneMarker::Kind::PendingReplace:
                /// The backup of a pending replacement is not loaded either, like one of a committed removal.
                pending_replaces.emplace(removed_name.value(), std::move(parsed.pending_replace));
                tombstones.emplace(std::move(removed_name.value()), std::nullopt);
                break;
            case PlainRewritableLayout::TombstoneMarker::Kind::Malformed:
                LOG_WARNING(log, "The marker '{}' of an unfinished operation is malformed: '{}'", marker->getPath(), content);
                malformed_markers.insert(std::move(removed_name.value()));
                break;
        }
    }

    auto throw_malformed_marker = [this](const std::string & name, const std::string & what)
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "The marker '{}' of an unfinished operation on {} is malformed, most likely because the process died while "
            "writing it. It is unknown whether the operation has to be rolled back or finished, so it has to be resolved manually",
            layout->constructTombstoneMarkerKey(name),
            what);
    };

    if (!tombstones.empty())
        LOG_DEBUG(log, "Found {} removals that were not finished", tombstones.size());

    const bool remove_orphaned_objects = is_initial_load && !object_storage->isReadOnly();
    const bool roll_back_pending_operations = is_initial_load || object_storage->isReadOnly();

    /// A pending replacement appears and disappears without a change to the `prefix.path` of the directory of its target,
    /// and the info of such a directory in the previous snapshot may describe the target as read from its backup. So a
    /// directory that has a pending replacement now, or had one at the previous load, is always loaded anew.
    std::unordered_set<std::string> current_remote_directories_of_pending_replaces;
    for (const auto & [_, pending_replace] : pending_replaces)
        current_remote_directories_of_pending_replaces.insert(pending_replace.directory_remote_path);
    std::unordered_set<std::string> directories_with_pending_replaces = current_remote_directories_of_pending_replaces;
    directories_with_pending_replaces.insert(remote_directories_of_pending_replaces.begin(), remote_directories_of_pending_replaces.end());

    /// The targets of pending replacements that a read-only disk reads from their backups.
    std::vector<std::pair<PlainRewritableLayout::PendingReplace, ObjectMetadata>> targets_read_from_backups;
    auto backups_of_targets = std::make_shared<BlobObjectKeyRemap>();

    /// A subsequent load of a writable disk leaves a pending replacement alone, because the move may be running in
    /// this very process.
    if (roll_back_pending_operations)
    {
        for (const auto & [removed_name, pending_replace] : pending_replaces)
        {
            const auto backup_key = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, removed_name);
            const auto target_key = layout->constructFileObjectKey(pending_replace.directory_remote_path, pending_replace.file_name);

            /// The marker is written before the backup, and the target is touched only once the backup is complete,
            /// so without a complete backup the target is intact. A backup of an unexpected size is kept together with
            /// its marker instead of being reclaimed: it may still be the only copy of the target.
            const auto backup_metadata = object_storage->tryGetObjectMetadata(backup_key, /*with_tags=*/ false);
            if (backup_metadata && backup_metadata->size_bytes != pending_replace.size)
            {
                if (backup_metadata->size_bytes < pending_replace.size && object_storage->exists(StoredObject(target_key)))
                {
                    LOG_INFO(log, "The backup '{}' of '{}' is incomplete ({} of {} bytes), so the target was not touched",
                        backup_key, target_key, backup_metadata->size_bytes, pending_replace.size);
                }
                else
                {
                    LOG_WARNING(log, "The backup '{}' of '{}' has {} bytes instead of {}, keeping it",
                        backup_key, target_key, backup_metadata->size_bytes, pending_replace.size);
                    kept_backups.insert(removed_name);
                }
            }
            else if (backup_metadata && !remove_orphaned_objects)
            {
                LOG_DEBUG(log, "Reading '{}' from its backup '{}' left by a replacement that was not committed", target_key, backup_key);
                backups_of_targets->emplace(target_key, backup_key);
                targets_read_from_backups.emplace_back(pending_replace, *backup_metadata);
            }
            else if (backup_metadata)
            {
                LOG_INFO(log, "Restoring '{}' from its backup '{}' left by a replacement that was not committed", target_key, backup_key);
                object_storage->copyObject(StoredObject(backup_key), StoredObject(target_key), settings, getWriteSettings());
            }
        }
    }

    /// The data objects of an orphaned subtree have to be deleted before its `prefix.path` objects, and the
    /// markers only after both, for the same reason as in `RemoveRecursiveOperation::finalize`: whatever is
    /// left after the process dies in the middle of this has to stay reachable and marked as garbage.
    StoredObjects orphaned_data_objects;
    StoredObjects orphaned_metadata_objects;
    /// The orphaned directories themselves: which of their blobs can be deleted is known only once the layout is complete.
    std::vector<DirectoryLoadResult> orphaned_directories;

    /// Whether the disk was large enough for the listings to be split into parallel requests.
    bool list_in_parallel = false;
    bool files_are_prelisted = false;

    /// The files of every directory of the disk, listed up front, and the slot every directory is loaded
    /// into. Both are declared before the runner so that they outlive the tasks that write into them.
    std::unordered_map<std::string, std::unordered_map<std::string, FileRemoteInfo>> prelisted_files;
    std::mutex prelisted_files_mutex;
    std::vector<DirectoryLoadResult> results;

    /// Record the files of every directory whose name starts with `name_prefix`. Listing by prefix makes
    /// one request cover many directories, and lets the listing of the disk be split into parallel parts.
    auto list_files_shard = [this, &prelisted_files, &prelisted_files_mutex](const std::string & name_prefix)
    {
        const std::string metadata_directory_prefix = layout->constructMetadataDirectoryKey() + "/";

        std::unordered_map<std::string, std::unordered_map<std::string, FileRemoteInfo>> shard;
        for (auto iterator = object_storage->iterate(layout->constructFilesDirectoryKey(name_prefix), 0, /*with_tags=*/ false, std::nullopt);
             iterator->isValid(); iterator->next())
        {
            const auto remote_file = iterator->current();
            const auto unpacked_remote_file_path = layout->parseFileObjectKey(remote_file->getPath());
            if (!unpacked_remote_file_path.has_value())
                continue;

            const auto & [directory_remote_path, filename] = unpacked_remote_file_path.value();
            /// A listing by prefix also reaches the metadata directory when a directory name starts with
            /// the same characters; the objects there describe directories, they are not files of one.
            if (directory_remote_path == PlainRewritableLayout::METADATA_DIRECTORY_TOKEN
                || remote_file->getPath().starts_with(metadata_directory_prefix))
                continue;

            shard[directory_remote_path].emplace(filename, FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
                .blob_key = {},
            });
        }

        std::lock_guard guard(prelisted_files_mutex);
        for (auto & [directory_remote_path, files] : shard)
            prelisted_files[directory_remote_path].merge(files);
    };

    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
    try
    {
        /// Enumerate the directories of the disk, that is, the `__meta/{directory}/prefix.path` objects.
        std::vector<DirectoryObject> directories;

        const std::string metadata_directory_key = layout->constructMetadataDirectoryKey();
        const bool can_list_by_prefix = object_storage->supportsPrefixListing();

        auto collect_directory = [&](const RelativePathWithMetadataPtr & file)
        {
            auto remote_path = layout->parseDirectoryObjectKey(file->getPath());
            if (remote_path.has_value())
                directories.emplace_back(DirectoryObject{file->getPath(), std::move(remote_path.value()), file->metadata});
        };

        /// A plain listing costs a single request for a small disk, which is the common case, so start
        /// with one and only switch to the sharded listing once the disk turns out to be large enough
        /// for the sequential pages to dominate the load time. The few pages read here are then re-read
        /// by the sharded listing; that is cheaper than always paying for one request per shard.
        for (auto iterator = object_storage->iterate(metadata_directory_key, 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            if (can_list_by_prefix && directories.size() >= MIN_DIRECTORIES_TO_LIST_IN_PARALLEL)
            {
                list_in_parallel = true;
                break;
            }
            collect_directory(iterator->current());
        }

        if (list_in_parallel)
        {
            directories.clear();
            std::mutex directories_mutex;

            auto list_shard = [&](const std::string & prefix, std::optional<std::string> start_after, bool stop_at_alphabet)
            {
                std::vector<DirectoryObject> shard;
                for (auto iterator = object_storage->iterate(prefix, 0, /*with_tags=*/ false, start_after); iterator->isValid(); iterator->next())
                {
                    const auto file = iterator->current();
                    auto remote_path = layout->parseDirectoryObjectKey(file->getPath());
                    if (!remote_path.has_value())
                        continue;
                    /// The shard that covers everything sorting before the alphabet must not go on to read
                    /// the whole directory once it reaches the alphabet - that is what it is splitting up.
                    if (stop_at_alphabet && !remote_path->empty()
                        && static_cast<unsigned char>(remote_path->front()) >= static_cast<unsigned char>(DIRECTORY_NAME_ALPHABET.front()))
                        break;
                    shard.emplace_back(DirectoryObject{file->getPath(), std::move(remote_path.value()), file->metadata});
                }

                std::lock_guard guard(directories_mutex);
                directories.insert(directories.end(), std::make_move_iterator(shard.begin()), std::make_move_iterator(shard.end()));
            };

            ThreadPoolCallbackRunnerLocal<void> listing_runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
            try
            {
                for (char c : DIRECTORY_NAME_ALPHABET)
                    listing_runner.enqueueAndKeepTrack([&, c] { list_shard(fmt::format("{}/{}", metadata_directory_key, c), std::nullopt, false); });

                /// Two more shards for the names that sort outside of the alphabet, so that a directory
                /// written by another implementation is never silently dropped from the listing.
                listing_runner.enqueueAndKeepTrack([&] { list_shard(metadata_directory_key, std::nullopt, true); });
                listing_runner.enqueueAndKeepTrack([&]
                {
                    const char after_alphabet = static_cast<char>(static_cast<unsigned char>(DIRECTORY_NAME_ALPHABET.back()) + 1);
                    list_shard(metadata_directory_key, fmt::format("{}/{}", metadata_directory_key, after_alphabet), false);
                });
            }
            catch (...)
            {
                listing_runner.waitForAllToFinish();
                throw;
            }
            listing_runner.waitForAllToFinishAndRethrowFirstError();
        }

        /// Listing the files of every directory separately costs one request per directory and dominates
        /// the load time of a large disk. Now that the directory names are known, the same objects can be
        /// enumerated by a few listings of the whole disk running in parallel.
        /// A refresh is excluded on purpose: it re-reads only the directories whose `prefix.path` changed,
        /// so it lists far fewer directories than the disk holds and listing the whole disk would be waste.
        files_are_prelisted = list_in_parallel && !do_not_load_unchanged_directories;
        std::vector<std::string> listing_shards;
        if (files_are_prelisted)
        {
            std::vector<std::string> directory_names;
            directory_names.reserve(directories.size());
            for (const auto & directory : directories)
                directory_names.push_back(directory.remote_path);

            listing_shards = makeListingShards(directory_names, directories.size() / DIRECTORIES_PER_LISTING_SHARD);
        }

        /// Reading the `prefix.path` objects is independent of listing the files, and neither of the two
        /// saturates the object storage on its own, so they are meant to run at the same time. The pool
        /// takes tasks in the order they were scheduled, so scheduling all the listings first would make
        /// them an earlier stage instead; the two kinds of task are interleaved to avoid that.
        results.resize(directories.size());
        size_t scheduled_shards = 0;
        const size_t shard_every = listing_shards.empty() ? 0 : std::max<size_t>(1, directories.size() / listing_shards.size());
        for (size_t i = 0; i < directories.size(); ++i)
        {
            if (scheduled_shards < listing_shards.size() && shard_every && i % shard_every == 0)
                runner.enqueueAndKeepTrack([&, name_prefix = listing_shards[scheduled_shards++]] { list_files_shard(name_prefix); });

            auto & directory = directories[i];

            /// Reuse the version observed by LIST. A rename/delete after this listing is
            /// observed by the next refresh; loading is not an atomic snapshot of remote metadata.
            if (do_not_load_unchanged_directories && directory.metadata->isEtagUsableAsCacheKey()
                && !directories_with_pending_replaces.contains(directory.remote_path))
            {
                if (const auto known_path = local_paths_by_remote_directory.find(directory.remote_path);
                    known_path != local_paths_by_remote_directory.end())
                {
                    if (auto known_info = read_snapshot->getDirectoryRemoteInfo(known_path->second);
                        known_info && known_info->remote_path == directory.remote_path && known_info->etag == directory.metadata->etag)
                    {
                        results[i] = DirectoryLoadResult{true, false, directory.object_path, known_path->second, std::move(*known_info)};
                        continue;
                    }
                }
            }

            /// Passing by reference:
            /// log: Created before runner, so it will be destroyed after
            /// settings: Same as log
            /// result: Same, and no two tasks are given the same slot
            /// In any case we have a try {} catch (...) around runner usage, so exceptions will call runner.waitForAllToFinish() first
            /// Thus the order of destruction of the variables is not important
            runner.enqueueAndKeepTrack([remote_path = std::move(directory.remote_path), object_path = std::move(directory.object_path), metadata = std::move(directory.metadata), read_snapshot, do_not_load_unchanged_directories, files_are_prelisted, roll_back_pending_operations, remove_orphaned_objects, &tombstones, &malformed_markers, &throw_malformed_marker, &directories_with_pending_replaces, &result = results[i], &log, &settings, this]
            {
                DB::setThreadName(ThreadName::PLAIN_REWRITABLE_META_LOAD);

                StoredObject object{object_path};
                String local_path;
                bool has_explicit_file_list = false;
                /// Assuming that local and the object storage clocks are synchronized.
                Poco::Timestamp last_modified = metadata->last_modified;
                std::unordered_map<std::string, FileRemoteInfo> files;
                bool is_orphaned = false;

                try
                {
                    if (metadata->size_bytes == 0)
                        LOG_TRACE(log, "The object with the key '{}' has size 0, skipping the read", object_path);
                    else
                    {
                        auto read_buf = object_storage->readObject(object, settings);
                        String contents;
                        readStringUntilEOF(contents, *read_buf);

                        auto prefix_path = parsePrefixPath(contents);
                        local_path = std::move(prefix_path.logical_path);
                        has_explicit_file_list = prefix_path.has_explicit_file_list;

                        /// In the explicit form, the list of files comes from the metadata object and the blobs are not listed.
                        for (auto & listed_file : prefix_path.files)
                        {
                            if (listed_file.blob_key == getDefaultBlobKey(remote_path, listed_file.name))
                                listed_file.blob_key.clear();

                            files.emplace(std::move(listed_file.name), FileRemoteInfo{
                                .bytes_size = listed_file.bytes_size,
                                .last_modified = last_modified.epochTime(),
                                .blob_key = std::move(listed_file.blob_key),
                            });
                        }
                    }

                    /// The root directory has a metadata object only in the explicit form, and it is stored under the reserved remote path.
                    if (remote_path == PlainRewritableLayout::ROOT_DIRECTORY_TOKEN)
                        local_path.clear();
                    else if (normalizePath(local_path).empty())
                    {
                        /// Only the reserved metadata object maps to the logical root, so an empty logical path here means that
                        /// the object does not describe a directory: it is either not written yet (`LocalObjectStorage` writes
                        /// to the final key directly, so an interrupted write can leave the object empty and visible),
                        /// or it is a leftover of a directory that has been removed. Loading it as the root would hide the real
                        /// root and send lookups under it to the prefix of this directory.
                        LOG_WARNING(log, "The object with the key '{}' does not contain the logical path of a directory, ignoring it", object_path);
                        return;
                    }

                    const auto removed_name = PlainRewritableLayout::getRemovedNameOfLocalPath(local_path);
                    if (removed_name && malformed_markers.contains(removed_name.value()))
                        throw_malformed_marker(removed_name.value(), fmt::format("the directory '{}' with the key '{}'", local_path, object_path));

                    const auto tombstone = removed_name ? tombstones.find(removed_name.value()) : tombstones.end();
                    if (tombstone != tombstones.end() && tombstone->second)
                    {
                        if (!roll_back_pending_operations)
                        {
                            LOG_TRACE(log, "The directory '{}' with the key '{}' is being removed, skipping", local_path, object_path);
                            return;
                        }

                        auto original_local_path = PlainRewritableLayout::restoreLocalPathOfPendingRemoval(local_path, tombstone->second.value());
                        LOG_TRACE(log, "The directory '{}' with the key '{}' was moved by a removal that was not committed, moving it back to '{}'",
                            local_path, object_path, original_local_path);

                        if (remove_orphaned_objects)
                        {
                            /// The explicit file list, if any, is kept: it is the only record of the files of the directory.
                            DirectoryRemoteInfo directory_info{.remote_path = remote_path, .etag = {}, .last_modified = 0, .files = files, .has_explicit_file_list = has_explicit_file_list};
                            auto write_buf = object_storage->writeObject(
                                object, WriteMode::Rewrite, /*object_attributes*/ std::nullopt, /*buf_size*/ 128, getWriteSettings());
                            writeString(serializePrefixPath(original_local_path, directory_info), *write_buf);
                            write_buf->finalize();
                        }

                        local_path = std::move(original_local_path);
                    }

                    is_orphaned = tombstone != tombstones.end() && !tombstone->second;
                    if (is_orphaned && !remove_orphaned_objects)
                    {
                        LOG_TRACE(log, "The directory '{}' with the key '{}' is being removed, skipping", local_path, object_path);
                        return;
                    }

                    if (do_not_load_unchanged_directories && !is_orphaned && !directories_with_pending_replaces.contains(remote_path))
                    {
                        if (const auto known_info = read_snapshot->getDirectoryRemoteInfo(local_path);
                            known_info && known_info->remote_path == remote_path && known_info->etag == metadata->etag)
                        {
                            result = DirectoryLoadResult{true, false, object_path, std::move(local_path), known_info.value()};
                            return;
                        }
                    }

                    /// In the implicit form, the files of the directory are the blobs stored under its prefix.
                    /// When they were listed up front, the listing may still be running, so they are taken from
                    /// `prelisted_files` afterwards.
                    if (!has_explicit_file_list && !files_are_prelisted)
                    {
                        for (auto dir_iterator = object_storage->iterate(layout->constructFilesDirectoryKey(remote_path), 0, /*with_tags=*/ false, std::nullopt); dir_iterator->isValid(); dir_iterator->next())
                        {
                            const auto remote_file = dir_iterator->current();
                            const auto unpacked_remote_file_path = layout->parseFileObjectKey(remote_file->getPath());
                            if (!unpacked_remote_file_path.has_value())
                            {
                                LOG_WARNING(log, "Legacy layout is in use, ignoring '{}'", remote_file->getPath());
                                continue;
                            }

                            const auto & [directory_remote_path, filename] = unpacked_remote_file_path.value();
                            chassert(directory_remote_path == remote_path);

                            files.emplace(filename, FileRemoteInfo{
                                .bytes_size = remote_file->metadata->size_bytes,
                                .last_modified = remote_file->metadata->last_modified.epochTime(),
                                .blob_key = {},
                            });
                        }
                    }

#if USE_AZURE_BLOB_STORAGE
                    fiu_do_on(FailPoints::plain_rewritable_object_storage_azure_not_found_on_init, {
                        std::bernoulli_distribution fault(0.25);
                        if (fault(thread_local_rng))
                        {
                            LOG_TEST(log, "Fault injection");
                            throw Azure::Storage::StorageException::CreateFromResponse(std::make_unique<Azure::Core::Http::RawResponse>(
                                1, 0, Azure::Core::Http::HttpStatusCode::NotFound, "Fault injected"));
                        }
                    });
#endif
                }
#if USE_AWS_S3
                catch (const S3Exception & e)
                {
                    /// It is ok if a directory was removed just now.
                    if (e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
                        return;
                    throw;
                }
#endif
#if USE_AZURE_BLOB_STORAGE
                catch (const Azure::Storage::StorageException & e)
                {
                    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
                        return;
                    throw;
                }
#endif
                catch (const ErrnoException & e)
                {
                    /// The same for the local object storage.
                    if (e.getErrno() == ENOENT)
                        return;
                    throw;
                }
                catch (...)
                {
                    throw;
                }

                result = DirectoryLoadResult{
                    true,
                    is_orphaned,
                    object_path,
                    std::move(local_path),
                    DirectoryRemoteInfo{remote_path, metadata->etag, last_modified.epochTime(), std::move(files), has_explicit_file_list}};
            });
        }

        for (; scheduled_shards < listing_shards.size(); ++scheduled_shards)
            runner.enqueueAndKeepTrack([&, name_prefix = listing_shards[scheduled_shards]] { list_files_shard(name_prefix); });
    }
    catch (...)
    {
        runner.waitForAllToFinish();
        throw;
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    /// Everything has been read by now, so the directories and their files can be put together.
    for (auto & result : results)
    {
        if (!result.loaded)
            continue;

        /// A directory with the explicit file list takes its files from the metadata object, not from the listing.
        if (files_are_prelisted && !result.info.has_explicit_file_list)
        {
            if (auto it = prelisted_files.find(result.info.remote_path); it != prelisted_files.end())
                result.info.files = std::move(it->second);
        }

        if (result.is_orphaned)
        {
            orphaned_directories.push_back(std::move(result));
            continue;
        }

        remote_layout[std::move(result.local_path)] = std::move(result.info);
    }

    /// Root folder is a special case. Files are stored as /__root/{file-name}, unless the root has switched to the explicit file list.
    /// The leftovers of the removals of files are stored there too, so it is listed even in the explicit form.
    {
        const bool root_has_explicit_file_list = remote_layout[""].has_explicit_file_list;
        for (auto iterator = object_storage->iterate(layout->constructRootFilesDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            auto remote_file = iterator->current();
            if (malformed_markers.contains(remote_file->getFileName()))
                throw_malformed_marker(remote_file->getFileName(), fmt::format("the object '{}'", remote_file->getPath()));

            /// Only a committed removal leaves a file in the root directory, a pending one is of a directory.
            if (auto it = tombstones.find(remote_file->getFileName()); it != tombstones.end() && !it->second)
            {
                if (remove_orphaned_objects && !kept_backups.contains(remote_file->getFileName()))
                    orphaned_data_objects.emplace_back(remote_file->getPath());
                continue;
            }

            if (root_has_explicit_file_list)
                continue;

            remote_layout[""].files.emplace(remote_file->getFileName(), FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
                .blob_key = {},
            });
        }
    }

    /// A blob of an orphaned directory may still be linked from a file outside of it (the removal kept such blobs),
    /// and an orphaned directory in the implicit form lists such a blob as its own file, so these blobs are not deleted.
    if (!orphaned_directories.empty())
    {
        std::unordered_set<std::string> referenced_blob_keys;
        for (const auto & [local_path, info] : remote_layout)
            for (const auto & [file_name, file_info] : info.files)
                referenced_blob_keys.insert(getBlobKey(info, file_name, file_info));

        for (const auto & orphaned_directory : orphaned_directories)
        {
            LOG_TRACE(log, "The directory '{}' with the key '{}' was not removed completely, its {} files will be removed",
                orphaned_directory.local_path, orphaned_directory.object_path, orphaned_directory.info.files.size());
            orphaned_metadata_objects.emplace_back(orphaned_directory.object_path);
            for (const auto & [file_name, file_info] : orphaned_directory.info.files)
            {
                const auto blob_key = getBlobKey(orphaned_directory.info, file_name, file_info);
                if (referenced_blob_keys.contains(blob_key))
                {
                    LOG_TRACE(log, "Keeping the blob '{}' of the orphaned file '{}', it is linked from elsewhere", blob_key, file_name);
                    continue;
                }
                orphaned_data_objects.emplace_back(layout->constructBlobObjectKey(blob_key));
            }
        }
    }

    LOG_DEBUG(log, "Loaded metadata for {} directories (listed {}, files listed {})",
        remote_layout.size(),
        list_in_parallel ? "by prefix shards" : "sequentially",
        files_are_prelisted ? "for the whole disk at once" : "per directory");

    std::unordered_map<std::string, std::string> new_local_paths;
    new_local_paths.reserve(remote_layout.size());
    for (const auto & [local_path, info] : remote_layout)
        new_local_paths.emplace(info.remote_path, local_path);

    /// The target may have been removed or overwritten with the source by now, but its committed content is the backup.
    for (const auto & [pending_replace, backup_metadata] : targets_read_from_backups)
    {
        const auto local_directory = new_local_paths.find(pending_replace.directory_remote_path);
        if (local_directory == new_local_paths.end())
            continue;

        remote_layout[local_directory->second].files.insert_or_assign(pending_replace.file_name, FileRemoteInfo{
            .bytes_size = backup_metadata.size_bytes,
            .last_modified = backup_metadata.last_modified.epochTime(),
            .blob_key = {},
        });
    }

    remote_directories_of_pending_replaces = std::move(current_remote_directories_of_pending_replaces);
    fs.applyLayout(std::move(remote_layout), std::move(backups_of_targets));
    local_paths_by_remote_directory = std::move(new_local_paths);
    previous_refresh.restart();

    if (remove_orphaned_objects && (!tombstones.empty() || !malformed_markers.empty()))
    {
        StoredObjects marker_objects;
        marker_objects.reserve(tombstones.size() + malformed_markers.size());
        /// Nothing on the disk has these names, otherwise the load would have failed above.
        for (const auto & removed_name : malformed_markers)
            marker_objects.emplace_back(layout->constructTombstoneMarkerKey(removed_name));
        size_t pending_removals = 0;
        for (const auto & [removed_name, pending_original_path] : tombstones)
        {
            if (kept_backups.contains(removed_name))
                continue;
            marker_objects.emplace_back(layout->constructTombstoneMarkerKey(removed_name));
            pending_removals += pending_original_path.has_value();
        }

        if (pending_removals)
            LOG_INFO(
                log,
                "Rolled back {} removals that were not committed, most likely because the process died in the middle of them",
                pending_removals);

        LOG_INFO(
            log,
            "Removing {} orphaned objects left by {} removals that were committed but not finished, most likely because the process died in the middle of them",
            orphaned_data_objects.size() + orphaned_metadata_objects.size(),
            tombstones.size() - pending_removals);

        object_storage->removeObjectsIfExist(orphaned_data_objects);
        object_storage->removeObjectsIfExist(orphaned_metadata_objects);
        /// Only now, when nothing of these removals is left, the markers can go: a marker that outlives its
        /// objects only costs another pass, while an object that outlives its marker would look like data.
        /// The same holds for a pending removal: its directories have all been moved back by now.
        object_storage->removeObjectsIfExist(marker_objects);

        ProfileEvents::increment(
            ProfileEvents::DiskPlainRewritableOrphanedObjectsRemoved,
            orphaned_data_objects.size() + orphaned_metadata_objects.size());
    }
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_, bool hard_links_enabled_)
    : object_storage(std::move(object_storage_))
    , metrics(createPlainRewritableMetrics(object_storage->getType()))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , hard_links_enabled(hard_links_enabled_)
    , fs(metrics->directory_map_size, metrics->file_count)
    , layout(std::make_shared<PlainRewritableLayout>(object_storage->getCommonKeyPrefix()))
{
    load(/*is_initial_load=*/true, /*do_not_load_unchanged_directories=*/false);
}

MetadataTransactionPtr MetadataStorageFromPlainRewritableObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainRewritableObjectStorageTransaction>(*this);
}

void MetadataStorageFromPlainRewritableObjectStorage::dropCache()
{
    std::unique_lock reload_lock(load_mutex);
    const auto tx_lock = path_locks.lockAll();
    load(/*is_initial_load=*/false, /*do_not_load_unchanged_directories=*/false);
}

void MetadataStorageFromPlainRewritableObjectStorage::refresh(UInt64 not_sooner_than_milliseconds)
{
    if (!previous_refresh.compareAndRestart(0.001 * static_cast<double>(not_sooner_than_milliseconds)))
        return;

    std::unique_lock load_lock(load_mutex, std::defer_lock);
    if (!load_lock.try_lock())
        return;

    /// The periodic refresh is best-effort. Waiting for the transactions in flight would make every new transaction
    /// wait behind this one, which is exactly the disk-wide stall during a long operation that the path locks avoid;
    /// so skip this round instead if anything is in flight. An explicit request (SYSTEM RESTART DISK passes zero
    /// to bypass the throttle) waits.
    const auto tx_lock = not_sooner_than_milliseconds == 0 ? std::optional(path_locks.lockAll()) : path_locks.tryLockAll();
    if (!tx_lock)
        return;

    load(/*is_initial_load=*/false, /*do_not_load_unchanged_directories=*/true);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->existsFile(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsDirectory(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->existsDirectory(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();
    return tree->existsFile(path) || tree->existsDirectory(path);
}

uint64_t MetadataStorageFromPlainRewritableObjectStorage::getFileSize(const std::string & path) const
{
    if (auto file_size = getFileSizeIfExists(path))
        return file_size.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<uint64_t> MetadataStorageFromPlainRewritableObjectStorage::getFileSizeIfExists(const std::string & path) const
{
    if (auto remote_info = fs.takeReadOnlySnapshot()->getFileRemoteInfo(path))
        return remote_info->bytes_size;

    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->listDirectory(path);
}

DirectoryIteratorPtr MetadataStorageFromPlainRewritableObjectStorage::iterateDirectory(const std::string & path) const
{
    auto paths = listDirectory(path);

    /// Prepend path, since iterateDirectory() includes path, unlike listDirectory()
    std::for_each(paths.begin(), paths.end(), [&](auto & child) { child = fs::path(path) / child; });
    std::vector<fs::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorage::getStorageObjects(const std::string & path) const
{
    if (auto objects = getStorageObjectsIfExist(path))
        return std::move(objects.value());

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<StoredObjects> MetadataStorageFromPlainRewritableObjectStorage::getStorageObjectsIfExist(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        return std::nullopt;

    const auto file_it = directory_remote_info->files.find(normalized_path.filename());
    if (file_it == directory_remote_info->files.end())
        return std::nullopt;

    auto object_key = layout->constructBlobObjectKey(getBlobKey(*directory_remote_info, normalized_path.filename(), file_it->second));
    if (object_storage->isReadOnly())
    {
        /// Taken from the same snapshot as the file, so that the size and the key of the blob come from the same load.
        if (const auto backups = tree->getBackupsOfPendingReplaceTargets())
            if (auto it = backups->find(object_key); it != backups->end())
                object_key = it->second;
    }

    return StoredObjects{StoredObject(object_key, path, file_it->second.bytes_size)};
}

uint32_t MetadataStorageFromPlainRewritableObjectStorage::getHardlinkCount(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);

    const auto file_it = directory_remote_info->files.find(normalized_path.filename());
    if (file_it == directory_remote_info->files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);

    /// As for the other metadata storages, this is the number of links besides the file itself.
    return tree->getBlobLinkCount(getBlobKey(*directory_remote_info, normalized_path.filename(), file_it->second)) - 1;
}

Poco::Timestamp MetadataStorageFromPlainRewritableObjectStorage::getLastModified(const std::string & path) const
{
    if (auto last_modified = getLastModifiedIfExists(path))
        return last_modified.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory {} does not exist", path);
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainRewritableObjectStorage::getLastModifiedIfExists(const String & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    if (tree->existsDirectory(path))
    {
        const auto remote_info = tree->getDirectoryRemoteInfo(path);

        if (remote_info)
            return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

        /// Let's return something in this case to unblock fs garbage cleanup.
        return Poco::Timestamp::fromEpochTime(0);
    }

    if (auto remote_info = tree->getFileRemoteInfo(path))
        return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

    return std::nullopt;
}

MetadataStorageFromPlainRewritableObjectStorageTransaction::MetadataStorageFromPlainRewritableObjectStorageTransaction(MetadataStorageFromPlainRewritableObjectStorage & metadata_storage_)
    : metadata_storage(metadata_storage_)
    , commit_snapshot(metadata_storage.fs.takeReadWriteSnapshot())
    , uncommitted_state(metadata_storage.fs.takeReadWriteSnapshot())
{
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    /// 0. Add preconditions for transaction commit and publishing of its result.
    operations.prependOperation(std::make_unique<MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation>(uncommitted_state.getTxPreconditions(), commit_snapshot));
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStoragePublishOperation>(
        commit_snapshot, metadata_storage.fs, metadata_storage.object_storage, metadata_storage.layout, removed_objects));

    {
        /// 1. Exclude the transactions touching the same paths, their ancestors or descendants, as well as full reloads.
        ///    The preconditions refer only to the affected paths and their ancestors, so they cannot be invalidated
        ///    while the locks are held. Transactions on unrelated paths proceed concurrently, including their
        ///    requests to the object storage, which may take a long time.
        const auto path_locks = metadata_storage.path_locks.lock(affected_paths);

        /// 2. Setup up-to-date fs into snapshot being used during commit, and start recording the changes made to it.
        commit_snapshot->resetToRoot(metadata_storage.fs.takeReadOnlySnapshot()->getRoot());

        /// 3. Execute all operations on top of write set.
        ///    The last operation replays the recorded changes on top of the latest fs, which may have been advanced
        ///    by concurrent transactions on unrelated paths in the meantime, and publishes the result.
        operations.commit();

        /// 4. Remove the temporary objects left by the operations (backup copies of unlinked and moved files, the
        ///    objects of removed directories). They live under reserved names with a tombstone marker, so a reload
        ///    never ingests them as metadata, even if this best-effort cleanup fails, and the next initial load
        ///    reclaims them (see `PlainRewritableLayout::REMOVED_NAME_PREFIX`). It still runs under the locks, so
        ///    that a full reload (`dropCache`), which takes the lock of the root, does not see a removal half-done.
        ///    The locks are per path, so this does not delay unrelated transactions.
        operations.finalize();
    }
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::addAffectedPath(const std::string & path)
{
    auto normalized_path = normalizePath(path).string();

    /// Operations on the root are no-ops, and a lock on the root would exclude every other transaction.
    if (!normalized_path.empty())
        affected_paths.push_back(std::move(normalized_path));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::addAffectedFilePath(const std::string & path)
{
    addAffectedPath(path);

    /// An operation on a file of a directory in the explicit form rewrites the whole `prefix.path` of the directory, which
    /// lists all of its files, so it must exclude the operations on the other files of the directory: otherwise one of
    /// two concurrent rewrites would lose the file of the other. The same holds for an operation that switches the
    /// directory to the explicit form, which happens with hard links enabled or when the blob of the file is shared.
    /// Without hard links, a directory never switches to the explicit form and a blob never becomes shared, so the
    /// state seen by this transaction cannot change in a way that matters (a change of the directory itself is caught
    /// by the preconditions).
    const auto normalized_path = normalizePath(path);
    const auto directory = normalized_path.parent_path();

    bool may_rewrite_directory_metadata = metadata_storage.hard_links_enabled;
    if (!may_rewrite_directory_metadata)
    {
        const auto & snapshot = uncommitted_state.getSnapshot();
        if (const auto directory_info = snapshot.getDirectoryRemoteInfo(directory))
        {
            may_rewrite_directory_metadata = directory_info->has_explicit_file_list;
            if (!may_rewrite_directory_metadata)
                if (const auto blob_key = getBlobKeyIfExists(snapshot, normalized_path))
                    may_rewrite_directory_metadata = snapshot.getBlobLinkCount(*blob_key) > 1;
        }
    }

    /// For a file in the root, this is the lock of the root, which excludes every other transaction.
    if (may_rewrite_directory_metadata)
        affected_paths.push_back(directory.string());
}

TransactionCommitOutcomeVariant MetadataStorageFromPlainRewritableObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & /*options*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain-Rewritable Metadata storage supports only commit");
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    addAffectedFilePath(path);

    /// The blob has been written to the key chosen by `generateObjectKeyForPath`; if the key was not generated
    /// by this transaction, the blob is expected at the default location.
    std::string blob_key;
    if (const auto it = generated_blob_keys.find(normalizePath(path).string()); it != generated_blob_keys.end())
        blob_key = it->second;

    /// The following operations of this transaction have to see the file: a hard link to it makes its blob shared,
    /// and then rewriting it has to pick a new blob instead of clobbering the shared one.
    uncommitted_state.recordCreatedFile(path, blob_key);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageWriteFileOperation>(
        path,
        objects.front(),
        std::move(blob_key),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectory(const std::string & path)
{
    if (normalizePath(path).empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    addAffectedPath(path);
    checkNotReservedPath(path, uncommitted_state);
    uncommitted_state.createDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/false,
        normalizeDirectoryPath(path),
        uncommitted_state.getDirectoryRemoteInfo(path)->remote_path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectoryRecursive(const std::string & path)
{
    if (normalizePath(path).empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    addAffectedPath(path);
    checkNotReservedPath(path, uncommitted_state);
    uncommitted_state.createDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/true,
        normalizeDirectoryPath(path),
        uncommitted_state.getDirectoryRemoteInfo(path)->remote_path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::markFallbackCopyMoved(const std::string & path_from, const std::string & path_to)
{
    /// The copy puts its blob at the default key of the original target, and the move carries it from there at commit.
    /// A rewrite of either path cannot supersede the copy anymore: the move needs it as the source, and the blob that
    /// the move or the copy produces at commit would overwrite the bytes that the caller writes before the commit.
    if (const auto it = fallback_copies.find(path_from); it != fallback_copies.end())
    {
        it->second.moved = true;
        fallback_copies[path_to] = FallbackCopy{.copy = it->second.copy, .moved = true};
    }
    else if (const auto it_to = fallback_copies.find(path_to); it_to != fallback_copies.end())
    {
        it_to->second.moved = true;
    }
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    addAffectedPath(path_from);
    addAffectedPath(path_to);
    checkNotReservedPath(path_to, uncommitted_state);
    uncommitted_state.moveDirectory(path_from, path_to);

    const auto directory_from = normalizePath(path_from).string();
    const auto directory_to = normalizePath(path_to).string();
    std::vector<std::pair<std::string, std::string>> moved_copies;
    for (const auto & [path, _] : fallback_copies)
        if (directory_from.empty() || path.starts_with(directory_from + "/"))
            moved_copies.emplace_back(path, (std::filesystem::path(directory_to) / path.substr(directory_from.empty() ? 0 : directory_from.size() + 1)).string());
    for (const auto & [moved_from, moved_to] : moved_copies)
        markFallbackCopyMoved(moved_from, moved_to);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(
        normalizeDirectoryPath(path_from),
        normalizeDirectoryPath(path_to),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::unlinkFile(const std::string & path, bool if_exists, bool /*should_remove_objects*/)
{
    addAffectedFilePath(path);

    const auto normalized_path = normalizePath(path);
    uncommitted_state.useDirectory(normalized_path.parent_path());

    /// Removing a file whose blob is shared switches the directory to the explicit file list.
    if (const auto blob_key = getBlobKeyIfExists(uncommitted_state.getSnapshot(), normalized_path);
        blob_key && uncommitted_state.getSnapshot().getBlobLinkCount(*blob_key) > 1)
        uncommitted_state.markDirectoryExplicit(normalized_path.parent_path());

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation>(
        path,
        if_exists,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeDirectory(const std::string & path)
{
    addAffectedPath(path);
    if (!normalizePath(path).empty())
        uncommitted_state.removeDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation>(
        normalizeDirectoryPath(path),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & /*should_remove_objects*/)
{
    addAffectedPath(path);
    if (!normalizePath(path).empty())
        uncommitted_state.removeDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation>(
        path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    addAffectedFilePath(path_from);
    addAffectedFilePath(path_to);
    checkNotReservedPath(path_to, uncommitted_state);

    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);
    uncommitted_state.useDirectory(normalized_path_from.parent_path());
    uncommitted_state.useDirectory(normalized_path_to.parent_path());

    /// A real hard link would make the metadata of the target directory unreadable by older servers, so it is opt-in.
    if (!metadata_storage.hard_links_enabled)
    {
        /// The copy is a new file with its own blob. In an implicit target directory the blob goes to the default location.
        /// A directory in the explicit form (written while hard links were enabled) may still have a blob at the default
        /// location that is linked from elsewhere after its file there was removed, so the copy gets a fresh random key,
        /// like any new file of such a directory (see `generateObjectKeyForPath`).
        std::string blob_key;
        if (const auto directory_to = uncommitted_state.getDirectoryRemoteInfo(normalized_path_to.parent_path());
            directory_to && directory_to->has_explicit_file_list)
            blob_key = getDefaultBlobKey(directory_to->remote_path, getRandomASCIIString(32));

        /// The following operations of this transaction have to see the copy, like any other created file: otherwise
        /// a rewrite of the target would plan as the creation of a missing file, and the copy would overwrite it at commit.
        uncommitted_state.recordCreatedFile(path_to, blob_key);

        auto copy = std::make_unique<MetadataStorageFromPlainObjectStorageCopyFileOperation>(
            path_from,
            path_to,
            std::move(blob_key),
            commit_snapshot,
            metadata_storage.object_storage,
            metadata_storage.layout,
            metadata_storage.metrics);
        fallback_copies[normalized_path_to.string()] = FallbackCopy{.copy = copy.get()};
        operations.addOperation(std::move(copy));
        return;
    }

    /// The target directory switches to the explicit file list and the blob becomes shared.
    uncommitted_state.recordHardLink(path_from, path_to);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageHardLinkOperation>(
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::planFileMove(const NormalizedPath & path_from, const NormalizedPath & path_to)
{
    markFallbackCopyMoved(path_from.string(), path_to.string());

    uncommitted_state.useDirectory(path_from.parent_path());
    uncommitted_state.useDirectory(path_to.parent_path());

    const auto & snapshot = uncommitted_state.getSnapshot();
    const auto directory_from = snapshot.getDirectoryRemoteInfo(path_from.parent_path());
    const auto directory_to = snapshot.getDirectoryRemoteInfo(path_to.parent_path());
    const auto blob_key_from = getBlobKeyIfExists(snapshot, path_from);
    if (!directory_from || !directory_to || !blob_key_from)
        return;

    const bool metadata_only = isMetadataOnlyMove(snapshot, *directory_from, *directory_to, *blob_key_from, getBlobKeyIfExists(snapshot, path_to));
    if (metadata_only)
    {
        /// A metadata-only move rewrites the `prefix.path` of both directories, even when only one of them is in the
        /// explicit form or only the source blob is shared, so both directories must be locked: `addAffectedFilePath`
        /// locks the directory of a path only by the state of that path. For a file in the root, this is the lock of the root.
        affected_paths.push_back(path_from.parent_path().string());
        affected_paths.push_back(path_to.parent_path().string());

        uncommitted_state.markDirectoryExplicit(path_from.parent_path());
        uncommitted_state.markDirectoryExplicit(path_to.parent_path());
    }

    /// The moved file has to be visible at its new path: a hard link to it makes its blob shared, and then rewriting it
    /// has to pick a new blob instead of clobbering the shared one.
    uncommitted_state.recordMovedFile(path_from, path_to, /*keeps_blob=*/metadata_only);
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    addAffectedFilePath(path_from);
    addAffectedFilePath(path_to);
    checkNotReservedPath(path_to, uncommitted_state);
    planFileMove(normalizePath(path_from), normalizePath(path_to));

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/false,
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    addAffectedFilePath(path_from);
    addAffectedFilePath(path_to);
    checkNotReservedPath(path_to, uncommitted_state);
    planFileMove(normalizePath(path_from), normalizePath(path_to));

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/true,
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

ObjectStorageKey MetadataStorageFromPlainRewritableObjectStorageTransaction::generateObjectKeyForPath(const std::string & path)
{
    checkNotReservedPath(path, uncommitted_state);
    const auto normalized_path = normalizePath(path);
    if (normalized_path.filename().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", path);

    /// The key is going to be used for writing the file, so the transaction depends on the parent directory,
    /// which is protected by the lock on the file path.
    addAffectedFilePath(path);

    /// The new blob replaces the copy that stood in for a hard link in this transaction, so the copy is redundant.
    /// It would also go to the same default key at commit (the target directory keeps the implicit form, where a file
    /// has one key) and overwrite the bytes that the caller writes to the key returned from here before the commit.
    if (const auto it = fallback_copies.find(normalized_path.string()); it != fallback_copies.end())
    {
        if (it->second.moved)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot write the file '{}': the transaction has moved the copy of a file that stands in for a hard link "
                "(the disk has hard links disabled) to or from this path",
                path);

        it->second.copy->supersede();
        fallback_copies.erase(it);
    }

    const auto parent_path = normalized_path.parent_path();
    const auto parent_info = uncommitted_state.getDirectoryRemoteInfo(parent_path);

    if (!parent_info)
    {
        /// Validate during commit that directory will be created on S3.
        uncommitted_state.useMissingDirectory(parent_path);

        /// Materialize virtual parent.
        createDirectoryRecursive(parent_path);
    }
    else
    {
        /// Validate during commit that directory will not be recreated on S3.
        uncommitted_state.useDirectory(parent_path);
    }

    if (const auto directory_remote_info = uncommitted_state.getDirectoryRemoteInfo(parent_path))
    {
        const auto file_name = normalized_path.filename().string();

        /// In a directory with the explicit file list the blob names are random: a new file must not clobber the blob
        /// of a removed file that is still linked from elsewhere. The same applies to rewriting a file whose blob is shared.
        bool use_random_name = directory_remote_info->has_explicit_file_list;
        if (!use_random_name)
            if (const auto it = directory_remote_info->files.find(file_name); it != directory_remote_info->files.end())
                use_random_name = uncommitted_state.getSnapshot().getBlobLinkCount(getBlobKey(*directory_remote_info, file_name, it->second)) > 1;

        auto blob_key = getDefaultBlobKey(directory_remote_info->remote_path, use_random_name ? getRandomASCIIString(32) : file_name);
        auto object_key = ObjectStorageKey::createAsAbsolute(metadata_storage.layout->constructBlobObjectKey(blob_key));
        generated_blob_keys[normalized_path.string()] = std::move(blob_key);
        return object_key;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", parent_path.string());
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return removed_objects;
}

}
