#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
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
#include <unordered_set>
#include <vector>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/SharedThreadPools.h>
#include <Poco/Timestamp.h>
#include <Common/CurrentMetrics.h>
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
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
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
            });
        }

        std::lock_guard guard(prelisted_files_mutex);
        for (auto & [directory_remote_path, files] : shard)
            prelisted_files[directory_remote_path].merge(files);
    };

    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
    try
    {
        /// Root folder is a special case. Files are stored as /__root/{file-name}.
        for (auto iterator = object_storage->iterate(layout->constructRootFilesDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            auto remote_file = iterator->current();
            remote_layout[""].files.emplace(remote_file->getFileName(), FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
            });
        }

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

            /// Passing by reference:
            /// log: Created before runner, so it will be destroyed after
            /// settings: Same as log
            /// result: Same, and no two tasks are given the same slot
            /// In any case we have a try {} catch (...) around runner usage, so exceptions will call runner.waitForAllToFinish() first
            /// Thus the order of destruction of the variables is not important
            runner.enqueueAndKeepTrack([remote_path = std::move(directory.remote_path), object_path = std::move(directory.object_path), metadata = std::move(directory.metadata), read_snapshot, do_not_load_unchanged_directories, files_are_prelisted, &result = results[i], &log, &settings, this]
            {
                DB::setThreadName(ThreadName::PLAIN_REWRITABLE_META_LOAD);

                StoredObject object{object_path};
                String local_path;
                /// Assuming that local and the object storage clocks are synchronized.
                Poco::Timestamp last_modified = metadata->last_modified;
                std::unordered_map<std::string, FileRemoteInfo> files;

                try
                {
                    if (metadata->size_bytes == 0)
                        LOG_TRACE(log, "The object with the key '{}' has size 0, skipping the read", object_path);
                    else
                    {
                        auto read_buf = object_storage->readObject(object, settings);
                        readStringUntilEOF(local_path, *read_buf);
                    }

                    if (do_not_load_unchanged_directories)
                    {
                        if (const auto known_info = read_snapshot->getDirectoryRemoteInfo(local_path);
                            known_info && known_info->remote_path == remote_path && known_info->etag == metadata->etag)
                        {
                            result = DirectoryLoadResult{true, std::move(local_path), known_info.value()};
                            return;
                        }
                    }

                    /// Load the list of files inside the directory. When they were listed up front, the
                    /// listing may still be running, so they are taken from `prelisted_files` afterwards.
                    if (!files_are_prelisted)
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
                catch (...)
                {
                    throw;
                }

                result = DirectoryLoadResult{
                    true,
                    std::move(local_path),
                    DirectoryRemoteInfo{remote_path, metadata->etag, last_modified.epochTime(), std::move(files)}};
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

        if (files_are_prelisted)
        {
            if (auto it = prelisted_files.find(result.info.remote_path); it != prelisted_files.end())
                result.info.files = std::move(it->second);
        }

        remote_layout[std::move(result.local_path)] = std::move(result.info);
    }

    LOG_DEBUG(log, "Loaded metadata for {} directories (listed {}, files listed {})",
        remote_layout.size(),
        list_in_parallel ? "by prefix shards" : "sequentially",
        files_are_prelisted ? "for the whole disk at once" : "per directory");
    fs.applyLayout(std::move(remote_layout));
    previous_refresh.restart();
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_)
    : object_storage(std::move(object_storage_))
    , metrics(createPlainRewritableMetrics(object_storage->getType()))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , fs(metrics->directory_map_size, metrics->file_count)
    , layout(std::make_shared<PlainRewritableLayout>(object_storage->getCommonKeyPrefix()))
{
    load(/*is_initial_load=*/true, /*do_not_load_unchanged_directories=*/false);
}

bool MetadataStorageFromPlainRewritableObjectStorage::isPathOnLocalFilesystem() const
{
    /// `storage_path_full` is an object key prefix inside the object storage, so it names a real
    /// local directory only when the object storage itself is backed by the local filesystem.
    return !object_storage->isRemote();
}

MetadataTransactionPtr MetadataStorageFromPlainRewritableObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainRewritableObjectStorageTransaction>(*this);
}

void MetadataStorageFromPlainRewritableObjectStorage::dropCache()
{
    std::unique_lock reload_lock(load_mutex);
    std::unique_lock tx_lock(metadata_mutex);
    load(/*is_initial_load=*/false, /*do_not_load_unchanged_directories=*/false);
}

void MetadataStorageFromPlainRewritableObjectStorage::refresh(UInt64 not_sooner_than_milliseconds)
{
    if (!previous_refresh.compareAndRestart(0.001 * static_cast<double>(not_sooner_than_milliseconds)))
        return;

    std::unique_lock load_lock(load_mutex, std::defer_lock);
    if (load_lock.try_lock())
    {
        std::unique_lock metadata_lock(metadata_mutex);
        load(/*is_initial_load=*/false, /*do_not_load_unchanged_directories=*/true);
    }
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

    const auto file_remote_info = tree->getFileRemoteInfo(path);
    if (!file_remote_info)
        return std::nullopt;

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        return std::nullopt;

    auto object_key = layout->constructFileObjectKey(directory_remote_info->remote_path, normalized_path.filename());
    return StoredObjects{StoredObject(object_key, path, file_remote_info->bytes_size)};
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
    , commit_snapshot(std::make_shared<FsSnapshot>())
    , uncommitted_state(metadata_storage.fs.takeReadWriteSnapshot())
{
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    /// 0. Add preconditions for transaction commit.
    operations.prependOperation(std::make_unique<MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation>(uncommitted_state.getTxPreconditions(), commit_snapshot));

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);

        /// 1. Setup up-to-date fs into snapshot being used during commit.
        commit_snapshot->setRoot(metadata_storage.fs.takeReadWriteSnapshot()->getRoot());

        /// 2. Execute all operations on top of write set.
        operations.commit();

        /// 3. Exchange metadata with updated fs.
        metadata_storage.fs.applySnapshot(commit_snapshot);
    }

    operations.finalize();
}

TransactionCommitOutcomeVariant MetadataStorageFromPlainRewritableObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & /*options*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain-Rewritable Metadata storage supports only commit");
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageWriteFileOperation>(
        path,
        objects.front(),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectory(const std::string & path)
{
    if (normalizePath(path).empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

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

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    uncommitted_state.moveDirectory(path_from, path_to);

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
    uncommitted_state.useDirectory(normalizePath(path).parent_path());

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
    uncommitted_state.useDirectory(normalizePath(path_from).parent_path());
    uncommitted_state.useDirectory(normalizePath(path_to).parent_path());

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCopyFileOperation>(
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    uncommitted_state.useDirectory(normalizePath(path_from).parent_path());
    uncommitted_state.useDirectory(normalizePath(path_to).parent_path());

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
    uncommitted_state.useDirectory(normalizePath(path_from).parent_path());
    uncommitted_state.useDirectory(normalizePath(path_to).parent_path());

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
    const auto normalized_path = normalizePath(path);
    if (normalized_path.filename().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", path);

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
        return ObjectStorageKey::createAsAbsolute(metadata_storage.layout->constructFileObjectKey(directory_remote_info->remote_path, normalized_path.filename()));

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", parent_path.string());
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return removed_objects;
}

}
