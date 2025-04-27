#include <Disks/ObjectStorages/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/ObjectStorages/InMemoryDirectoryPathMap.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>

#include <any>
#include <cstddef>
#include <exception>
#include <iterator>
#include <optional>
#include <unordered_set>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/SharedThreadPools.h>
#include <Storages/PartitionCommands.h>
#include <Poco/Timestamp.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>

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
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char plain_rewritable_object_storage_azure_not_found_on_init[];
}

namespace
{

constexpr auto PREFIX_PATH_FILE_NAME = "prefix.path";
constexpr auto METADATA_PATH_TOKEN = "__meta/";

}


void MetadataStorageFromPlainRewritableObjectStorage::load(bool is_initial_load)
{
    ThreadPool & pool = getIOThreadPool().get();
    ThreadPoolCallbackRunnerLocal<void> runner(pool, "PlainRWMetaLoad");

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.remote_fs_method = RemoteFSReadMethod::threadpool;
    settings.remote_fs_prefetch = false;
    settings.remote_fs_buffer_size = 1024;  /// These files are small.

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

    size_t num_dirs_found = 0;
    size_t num_dirs_added = 0;
    size_t num_dirs_removed = 0;

    std::set<std::string> set_of_remote_paths;

    bool has_metadata = object_storage->existsOrHasAnyChild(metadata_key_prefix);

    if (is_initial_load)
    {
        /// Use iteration to determine if the disk contains data.
        /// LocalObjectStorage creates an empty top-level directory even when no data is stored,
        /// unlike blob storage, which has no concept of directories, therefore existsOrHasAnyChild
        /// is not applicable.
        auto common_key_prefix = fs::path(object_storage->getCommonKeyPrefix()) / "";
        bool has_data = object_storage->isRemote() ? object_storage->existsOrHasAnyChild(common_key_prefix) : object_storage->iterate(common_key_prefix, 0)->isValid();
        /// No metadata directory: legacy layout is likely in use.
        if (has_data && !has_metadata)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableLegacyLayoutDiskCount, 1);
            LOG_WARNING(log, "Legacy layout is likely used for disk '{}'", object_storage->getCommonKeyPrefix());
        }

        if (!has_metadata)
        {
            LOG_DEBUG(log, "Loaded metadata (empty)");
            return;
        }
    }

    try
    {
        for (auto iterator = object_storage->iterate(metadata_key_prefix, 0); iterator->isValid(); iterator->next())
        {
            auto file = iterator->current();
            String path = file->getPath();

            /// __meta/randomlygenerated/prefix.path
            auto remote_metadata_path = std::filesystem::path(path);
            if (remote_metadata_path.filename() != PREFIX_PATH_FILE_NAME)
                continue;

            chassert(remote_metadata_path.has_parent_path());
            chassert(remote_metadata_path.string().starts_with(metadata_key_prefix));

            /// randomlygenerated/prefix.path
            auto suffix = remote_metadata_path.string().substr(metadata_key_prefix.size());
            auto rel_path = std::filesystem::path(std::move(suffix));

            /// randomlygenerated
            auto remote_path = rel_path.parent_path();
            set_of_remote_paths.insert(remote_path);

            ++num_dirs_found;
            if (path_map->existsRemotePathUnchanged(remote_path, file->metadata->etag))
            {
                /// Already loaded.
                continue;
            }

            ++num_dirs_added;
            runner([remote_metadata_path, remote_path, path, metadata = file->metadata, &log, &settings, this]
            {
                setThreadName("PlainRWMetaLoad");

                StoredObject object{path};
                String local_path;
                /// Assuming that local and the object storage clocks are synchronized.
                Poco::Timestamp last_modified = metadata->last_modified;
                InMemoryDirectoryPathMap::FileNames files;

                try
                {
                    auto read_buf = object_storage->readObject(object, settings);
                    readStringUntilEOF(local_path, *read_buf);

                    /// Load the list of files inside the directory.
                    fs::path full_remote_path = object_storage->getCommonKeyPrefix() / remote_path;
                    size_t full_prefix_length = full_remote_path.string().size() + 1; /// common/key/prefix/randomlygenerated/
                    for (auto dir_iterator = object_storage->iterate(full_remote_path, 0); dir_iterator->isValid(); dir_iterator->next())
                    {
                        auto remote_file = dir_iterator->current();
                        String remote_file_path = remote_file->getPath();
                        chassert(remote_file_path.starts_with(full_remote_path.string()));
                        auto filename = fs::path(remote_file_path).filename();
                        /// Skip metadata files.
                        if (filename == PREFIX_PATH_FILE_NAME)
                        {
                            LOG_WARNING(log, "Legacy layout is in use, ignoring '{}'", remote_file_path);
                            continue;
                        }

                        /// Check that the file is a direct child.
                        chassert(full_prefix_length < remote_file_path.size());
                        if (std::string_view(remote_file_path.data() + full_prefix_length) == filename)
                            files.insert(std::move(filename));
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

                path_map->addOrReplacePath(
                    fs::path(local_path).parent_path(),
                    InMemoryDirectoryPathMap::RemotePathInfo{remote_path, metadata->etag, last_modified.epochTime(), std::move(files)});
            });
        }
    }
    catch (...)
    {
        runner.waitForAllToFinish();
        throw;
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    /// Now check which paths have to be removed in memory.
    num_dirs_removed = path_map->removeOutdatedPaths(set_of_remote_paths);

    size_t num_dirs_in_memory = path_map->directoriesCount();

    LOG_DEBUG(log, "Loaded metadata for {} directories ({} currently, {} added, {} removed)",
        num_dirs_found, num_dirs_in_memory, num_dirs_added, num_dirs_removed);

    previous_refresh.restart();
}

void MetadataStorageFromPlainRewritableObjectStorage::refresh(UInt64 not_sooner_than_milliseconds)
{
    if (!previous_refresh.compareAndRestart(0.001 * not_sooner_than_milliseconds))
        return;

    std::unique_lock lock(load_mutex, std::defer_lock);
    if (lock.try_lock())
        load(/*is_initial_load*/ false);
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t object_metadata_cache_size)
    : MetadataStorageFromPlainObjectStorage(object_storage_, storage_path_prefix_, object_metadata_cache_size)
    , metadata_key_prefix(std::filesystem::path(object_storage->getCommonKeyPrefix()) / METADATA_PATH_TOKEN)
    , path_map(std::make_shared<InMemoryDirectoryPathMap>(
        object_storage->getMetadataStorageMetrics().directory_map_size,
        object_storage->getMetadataStorageMetrics().file_count))
{
    if (object_storage->isWriteOnce())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MetadataStorageFromPlainRewritableObjectStorage is not compatible with write-once storage '{}'",
            object_storage->getName());

    load(/*is_initial_load*/ true);

    /// Use flat directory structure if the metadata is stored separately from the table data.
    auto keys_gen = std::make_shared<FlatDirectoryStructureKeyGenerator>(object_storage->getCommonKeyPrefix(), path_map);
    object_storage->setKeysGenerator(keys_gen);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    if (existsDirectory(path))
        return true;

    return getObjectMetadataEntryWithCache(path) != nullptr;
}

bool MetadataStorageFromPlainRewritableObjectStorage::supportsPartitionCommand(const PartitionCommand & command) const
{
    return command.type == PartitionCommand::DROP_PARTITION || command.type == PartitionCommand::DROP_DETACHED_PARTITION
        || command.type == PartitionCommand::ATTACH_PARTITION || command.type == PartitionCommand::MOVE_PARTITION
        || command.type == PartitionCommand::REPLACE_PARTITION;
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    if (existsDirectory(path))
        return false;

    return getObjectMetadataEntryWithCache(path) != nullptr;
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsDirectory(const std::string & path) const
{
    return path_map->getRemotePathInfoIfExists(path) != std::nullopt;
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    std::unordered_set<std::string> result = getDirectChildrenOnDisk(fs::path(path) / "");
    return std::vector<std::string>(std::make_move_iterator(result.begin()), std::make_move_iterator(result.end()));
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainRewritableObjectStorage::getLastModifiedIfExists(const String & path) const
{
    /// Path corresponds to a directory.
    if (auto remote = path_map->getRemotePathInfoIfExists(path))
        return Poco::Timestamp::fromEpochTime(remote->last_modified);

    /// A file.
    if (auto res = getObjectMetadataEntryWithCache(path))
        return Poco::Timestamp::fromEpochTime(res->last_modified);
    return std::nullopt;
}

std::unordered_set<std::string>
MetadataStorageFromPlainRewritableObjectStorage::getDirectChildrenOnDisk(const fs::path & local_path) const
{
    std::unordered_set<std::string> result;
    path_map->iterateSubdirectories(local_path, [&](const auto & elem){ result.emplace(elem); });
    path_map->iterateFiles(local_path, [&](const auto & elem){ result.emplace(elem); });
    return result;
}

}
