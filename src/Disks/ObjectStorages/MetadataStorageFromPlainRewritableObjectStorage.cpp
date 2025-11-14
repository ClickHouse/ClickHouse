#include <Disks/ObjectStorages/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>

#include <cstddef>
#include <optional>
#include <unordered_set>
#include <vector>
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
constexpr auto ROOT_FOLDER_TOKEN = "__root";

}


void MetadataStorageFromPlainRewritableObjectStorage::load(bool is_initial_load)
{
    ThreadPool & pool = getIOThreadPool().get();
    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);

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

    bool has_metadata = object_storage->existsOrHasAnyChild(metadata_key_prefix);

    std::mutex remote_layout_mutex;
    std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout;
    remote_layout[""] = DirectoryRemoteInfo{ROOT_FOLDER_TOKEN, "fake_etag", 0, {}};

    if (is_initial_load)
    {
        /// Use iteration to determine if the disk contains data.
        /// LocalObjectStorage creates an empty top-level directory even when no data is stored,
        /// unlike blob storage, which has no concept of directories, therefore existsOrHasAnyChild
        /// is not applicable.
        auto common_key_prefix = fs::path(object_storage->getCommonKeyPrefix()) / "";
        bool has_data = object_storage->isRemote() ? object_storage->existsOrHasAnyChild(common_key_prefix) : object_storage->iterate(common_key_prefix, 0, /*with_tags=*/ false)->isValid();
        /// No metadata directory: legacy layout is likely in use.
        if (has_data && !has_metadata)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableLegacyLayoutDiskCount, 1);
            LOG_WARNING(log, "Legacy layout is likely used for disk '{}'", object_storage->getCommonKeyPrefix());
        }

        if (!has_data && !has_metadata)
        {
            LOG_DEBUG(log, "Loaded metadata (empty)");
            fs_tree->apply(std::move(remote_layout));
            return;
        }
    }

    try
    {
        /// Root folder is a special case. Files are stored as /__root/{file-name}.
        for (auto iterator = object_storage->iterate(std::filesystem::path(object_storage->getCommonKeyPrefix()) / ROOT_FOLDER_TOKEN, 0, /*with_tags=*/ false); iterator->isValid(); iterator->next())
            remote_layout[""].file_names.insert(fs::path(iterator->current()->getPath()).filename());

        for (auto iterator = object_storage->iterate(metadata_key_prefix, 0, /*with_tags=*/ false); iterator->isValid(); iterator->next())
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

            if (auto directory_info = fs_tree->lookupDirectoryIfNotChanged(remote_path, file->metadata->etag))
            {
                /// Already loaded.
                std::lock_guard guard(remote_layout_mutex);
                auto & [local_path, remote_info] = directory_info.value();
                remote_layout[local_path] = std::move(remote_info);
                continue;
            }

            runner([remote_metadata_path, remote_path, path, metadata = file->metadata, &log, &settings, this, &remote_layout, &remote_layout_mutex]
            {
                DB::setThreadName(ThreadName::PLAIN_REWRITABLE_META_LOAD);

                StoredObject object{path};
                String local_path;
                /// Assuming that local and the object storage clocks are synchronized.
                Poco::Timestamp last_modified = metadata->last_modified;
                std::unordered_set<std::string> files;

                try
                {
                    if (metadata->size_bytes == 0)
                        LOG_TRACE(log, "The object with the key '{}' has size 0, skipping the read", remote_metadata_path);
                    else
                    {
                        auto read_buf = object_storage->readObject(object, settings);
                        readStringUntilEOF(local_path, *read_buf);
                    }

                    /// Load the list of files inside the directory.
                    fs::path full_remote_path = object_storage->getCommonKeyPrefix() / remote_path;
                    size_t full_prefix_length = full_remote_path.string().size() + 1; /// common/key/prefix/randomlygenerated/
                    for (auto dir_iterator = object_storage->iterate(full_remote_path, 0, /*with_tags=*/ false); dir_iterator->isValid(); dir_iterator->next())
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

                std::lock_guard guard(remote_layout_mutex);
                remote_layout[local_path] = DirectoryRemoteInfo{remote_path, metadata->etag, last_modified.epochTime(), std::move(files)};
            });
        }
    }
    catch (...)
    {
        runner.waitForAllToFinish();
        throw;
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    LOG_DEBUG(log, "Loaded metadata for {} directories", remote_layout.size());
    fs_tree->apply(std::move(remote_layout));
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
    , fs_tree(std::make_shared<InMemoryDirectoryTree>(
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
    auto keys_gen = std::make_shared<FlatDirectoryStructureKeyGenerator>(object_storage->getCommonKeyPrefix(), fs_tree);
    object_storage->setKeysGenerator(keys_gen);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    return existsDirectory(path) || existsFile(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    return fs_tree->existsFile(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsDirectory(const std::string & path) const
{
    return fs_tree->existsDirectory(path).first;
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    return fs_tree->listDirectory(path);
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainRewritableObjectStorage::getLastModifiedIfExists(const String & path) const
{
    if (auto [exists, remote_info] = fs_tree->existsDirectory(path); exists)
    {
        if (remote_info)
            return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

        /// Let's return something in this case to unblock fs garbage cleanup.
        return Poco::Timestamp::fromEpochTime(0);
    }

    if (auto object_metadata = getObjectMetadataEntryWithCache(path))
        return Poco::Timestamp::fromEpochTime(object_metadata->last_modified);

    return std::nullopt;
}

}
