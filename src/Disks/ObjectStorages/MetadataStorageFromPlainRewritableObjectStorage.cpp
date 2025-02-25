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
#include <Poco/Timestamp.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>

#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/storage_exception.hpp>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
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


void MetadataStorageFromPlainRewritableObjectStorage::load()
{
    ThreadPool & pool = getIOThreadPool().get();
    ThreadPoolCallbackRunnerLocal<void> runner(pool, "PlainRWMetaLoad");

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.remote_fs_method = RemoteFSReadMethod::read;
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

    size_t num_dirs_found = 0;
    size_t num_dirs_added = 0;
    size_t num_dirs_removed = 0;

    std::set<std::string> set_of_remote_paths;

    if (!object_storage->existsOrHasAnyChild(metadata_key_prefix))
    {
        LOG_DEBUG(log, "Loaded metadata (empty)");
        return;
    }

    try
    {
        for (auto iterator = object_storage->iterate(metadata_key_prefix, 0); iterator->isValid(); iterator->next())
        {
            ++num_dirs_found;
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

            if (path_map->existsRemotePath(remote_path))
            {
                /// Already loaded.
                continue;
            }

            ++num_dirs_added;
            runner([remote_metadata_path, remote_path, path, &log, &settings, this]
            {
                setThreadName("PlainRWMetaLoad");

                StoredObject object{path};
                String local_path;
                Poco::Timestamp last_modified{};
                InMemoryDirectoryPathMap::Files files;

                try
                {
                    auto read_buf = object_storage->readObject(object, settings);
                    readStringUntilEOF(local_path, *read_buf);
                    auto object_metadata = object_storage->tryGetObjectMetadata(path);

                    /// It ok if a directory was removed just now.
                    /// We support attaching a filesystem that is concurrently modified by someone else.
                    if (!object_metadata)
                        return;

                    /// Assuming that local and the object storage clocks are synchronized.
                    last_modified = object_metadata->last_modified;

                    /// Load the list of files inside the directory
                    fs::path full_remote_path = object_storage->getCommonKeyPrefix() / remote_path;
                    size_t prefix_length = remote_path.string().size() + 1; /// randomlygenerated/
                    for (auto dir_iterator = object_storage->iterate(full_remote_path, 0); dir_iterator->isValid(); dir_iterator->next())
                    {
                        auto remote_file = dir_iterator->current();
                        String remote_file_path = remote_file->getPath();
                        chassert(remote_file_path.starts_with(full_remote_path.string()));
                        auto filename = fs::path(remote_file_path).filename();

                        /// Check that the file is a direct child.
                        if (remote_file_path.substr(prefix_length) == filename)
                        {
                            size_t file_size = remote_file->metadata ? remote_file->metadata->size_bytes : 0;
                            files.emplace(std::move(filename), file_size);
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

                auto added = path_map->addPathIfNotExists(
                    fs::path(local_path).parent_path(),
                    InMemoryDirectoryPathMap::RemotePathInfo{remote_path, last_modified.epochTime(), std::move(files)});

                /// This can happen if table replication is enabled, then the same local path is written
                /// in `prefix.path` of each replica.
                if (!added.second)
                {
                    LOG_WARNING(
                        log,
                        "The local path '{}' is already mapped to a remote path '{}', ignoring: '{}'",
                        local_path,
                        added.first->second.path,
                        remote_path);
                }
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
    load();
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_)
    : MetadataStorageFromPlainObjectStorage(object_storage_, storage_path_prefix_, 0)
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

    load();

    /// Use flat directory structure if the metadata is stored separately from the table data.
    auto keys_gen = std::make_shared<FlatDirectoryStructureKeyGenerator>(object_storage->getCommonKeyPrefix(), path_map);
    object_storage->setKeysGenerator(keys_gen);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    return existsDirectory(path) || existsFile(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    auto fs_path = fs::path(path);
    auto dir = fs_path.parent_path();
    auto info = path_map->getRemotePathInfoIfExists(dir);
    if (!info)
        return false;

    auto filename = fs_path.filename();
    return info->files.contains(filename);
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

uint64_t MetadataStorageFromPlainRewritableObjectStorage::getFileSize(const String & path) const
{
    auto res = getFileSizeIfExists(path);
    if (!res)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist on {}", path, object_storage->getName());
    return *res;
}

std::optional<uint64_t> MetadataStorageFromPlainRewritableObjectStorage::getFileSizeIfExists(const String & path) const
{
    auto fs_path = fs::path(path);
    auto dir = fs_path.parent_path();
    auto filename = fs_path.filename();
    if (auto remote = path_map->getRemotePathInfoIfExists(dir))
        if (auto it = remote->files.find(filename); it != remote->files.end())
            return it->second;
    return std::nullopt;
}

Poco::Timestamp MetadataStorageFromPlainRewritableObjectStorage::getLastModified(const std::string & path) const
{
    auto res = getLastModifiedIfExists(path);
    if (!res)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist on {}", path, object_storage->getName());
    return *res;
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainRewritableObjectStorage::getLastModifiedIfExists(const String & path) const
{
    /// Path corresponds to a directory.
    if (auto remote = path_map->getRemotePathInfoIfExists(path))
        return Poco::Timestamp::fromEpochTime(remote->last_modified);

    /// A file. We don't want to store precise modifications time, so return the modification time of the parent directory.
    auto fs_path = fs::path(path);
    auto dir = fs_path.parent_path();
    if (auto remote = path_map->getRemotePathInfoIfExists(dir))
        return Poco::Timestamp::fromEpochTime(remote->last_modified);
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
