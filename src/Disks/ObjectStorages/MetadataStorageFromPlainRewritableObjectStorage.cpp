#include <Disks/ObjectStorages/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/ObjectStorages/InMemoryDirectoryPathMap.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>

#include <cstddef>
#include <exception>
#include <iterator>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/SharedThreadPools.h>
#include <Poco/Timestamp.h>
#include "Common/Exception.h"
#include "Common/Logger.h"
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Common/logger_useful.h>
#include "CommonPathPrefixKeyGenerator.h"
#include "Disks/ObjectStorages/IObjectStorage_fwd.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr auto PREFIX_PATH_FILE_NAME = "prefix.path";
constexpr auto METADATA_PATH_TOKEN = "__meta/";

/// Use a separate layout for metadata if:
/// 1. The disk endpoint does not contain any objects yet (empty), OR
/// 2. The metadata is already stored behind a separate endpoint.
/// Otherwise, store metadata along with regular data for backward compatibility.
std::string getMetadataKeyPrefix(ObjectStoragePtr object_storage)
{
    const auto common_key_prefix = std::filesystem::path(object_storage->getCommonKeyPrefix());
    const auto metadata_key_prefix = std::filesystem::path(common_key_prefix) / METADATA_PATH_TOKEN;
    return !object_storage->existsOrHasAnyChild(metadata_key_prefix / "") && object_storage->existsOrHasAnyChild(common_key_prefix / "")
        ? common_key_prefix
        : metadata_key_prefix;
}

void loadDirectoryTree(InMemoryDirectoryPathMap::Map & map, ObjectStoragePtr object_storage)
{
    const auto common_key_prefix = object_storage->getCommonKeyPrefix();
    LOG_DEBUG(getLogger("MetadataStorageFromPlainObjectStorage"), "Loading directory structure");
    for (auto & [local_path, info] : map)
    {
        LOG_TRACE(getLogger("loadDirectory"), "Loading directories for local path: {}", local_path);
        const auto remote_path = std::filesystem::path(common_key_prefix) / info.path / "";
        for (auto iterator = object_storage->iterate(remote_path, 0); iterator->isValid(); iterator->next())
        {
            auto file = iterator->current();
            String path = file->getPath();
            LOG_TRACE(getLogger("loadDirectory"), "Remote path: {}", path);
            chassert(path.starts_with(remote_path.string()));
            auto filename = std::filesystem::path(path).filename();
            /// Check that the file is a direct child.
            if (path.substr(remote_path.string().size()) == filename)
                info.filenames.emplace(filename);
        }
    }
}

std::shared_ptr<InMemoryDirectoryPathMap> loadPathPrefixMap(const std::string & metadata_key_prefix, ObjectStoragePtr object_storage)
{
    auto result = std::make_shared<InMemoryDirectoryPathMap>();
    using Map = InMemoryDirectoryPathMap::Map;

    ThreadPool & pool = getIOThreadPool().get();
    ThreadPoolCallbackRunnerLocal<void> runner(pool, "PlainRWMetaLoad");

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.remote_fs_method = RemoteFSReadMethod::read;
    settings.remote_fs_buffer_size = 1024;  /// These files are small.

    LOG_DEBUG(log, "Loading metadata");
    size_t num_files = 0;

    std::mutex mutex;
    InMemoryDirectoryPathMap::Map map;
    for (auto iterator = object_storage->iterate(metadata_key_prefix, 0); iterator->isValid(); iterator->next())
    {
        ++num_files;
        auto file = iterator->current();
        String path = file->getPath();
        auto remote_metadata_path = std::filesystem::path(path);
        if (remote_metadata_path.filename() != PREFIX_PATH_FILE_NAME)
            continue;

        runner(
            [remote_metadata_path, path, &object_storage, &mutex, &map, &log, &settings, &metadata_key_prefix]
            {
                setThreadName("PlainRWMetaLoad");

                StoredObject object{path};
                String local_path;
                Poco::Timestamp last_modified{};

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
                catch (...)
                {
                    throw;
                }

                chassert(remote_metadata_path.has_parent_path());
                chassert(remote_metadata_path.string().starts_with(metadata_key_prefix));
                auto suffix = remote_metadata_path.string().substr(metadata_key_prefix.size());
                auto rel_path = std::filesystem::path(std::move(suffix));
                std::pair<Map::iterator, bool> res;
                {
                    std::lock_guard lock(mutex);
                    res = map.emplace(
                        std::filesystem::path(local_path).parent_path(),
                        InMemoryDirectoryPathMap::RemotePathInfo{rel_path.parent_path(), last_modified.epochTime(), {}});
                }

                /// This can happen if table replication is enabled, then the same local path is written
                /// in `prefix.path` of each replica.
                if (!res.second)
                    LOG_WARNING(
                        log,
                        "The local path '{}' is already mapped to a remote path '{}', ignoring: '{}'",
                        local_path,
                        res.first->second.path,
                        rel_path.parent_path().string());
            });
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    loadDirectoryTree(map, object_storage);
    {
        std::lock_guard lock(result->mutex);
        result->map = std::move(map);
        LOG_DEBUG(log, "Loaded metadata for {} files, found {} directories", num_files, result->map.size());

        auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
        CurrentMetrics::add(metric, result->map.size());
    }
    return result;
}

}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t object_metadata_cache_size)
    : MetadataStorageFromPlainObjectStorage(object_storage_, storage_path_prefix_, object_metadata_cache_size)
    , metadata_key_prefix(DB::getMetadataKeyPrefix(object_storage))
    , path_map(loadPathPrefixMap(metadata_key_prefix, object_storage))
{
    if (object_storage->isWriteOnce())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MetadataStorageFromPlainRewritableObjectStorage is not compatible with write-once storage '{}'",
            object_storage->getName());

    if (useSeparateLayoutForMetadata())
    {
        /// Use flat directory structure if the metadata is stored separately from the table data.
        auto keys_gen = std::make_shared<FlatDirectoryStructureKeyGenerator>(object_storage->getCommonKeyPrefix(), path_map);
        object_storage->setKeysGenerator(keys_gen);
    }
    else
    {
        auto keys_gen = std::make_shared<CommonPathPrefixKeyGenerator>(object_storage->getCommonKeyPrefix(), path_map);
        object_storage->setKeysGenerator(keys_gen);
    }
}

MetadataStorageFromPlainRewritableObjectStorage::~MetadataStorageFromPlainRewritableObjectStorage()
{
    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::sub(metric, path_map->map.size());
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    if (existsDirectory(path))
        return true;

    return getObjectMetadataEntryWithCache(path) != nullptr;
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
    std::unordered_set<std::string> result = getDirectChildrenOnDisk(std::filesystem::path(path) / "");
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
MetadataStorageFromPlainRewritableObjectStorage::getDirectChildrenOnDisk(const std::filesystem::path & local_path) const
{
    std::unordered_set<std::string> result;
    SharedLockGuard lock(path_map->mutex);
    // const auto & map = path_map->map;
    const auto end_it = path_map->map.end();
    /// Directories.
    for (auto it = path_map->map.lower_bound(local_path); it != end_it; ++it)
    {
        const auto & [k, _] = std::make_tuple(it->first.string(), it->second);
        if (!k.starts_with(local_path.string()))
            break;

        auto slash_num = count(k.begin() + local_path.string().size(), k.end(), '/');
        /// The directory map comparator ensures that the paths with the smallest number of
        /// hops from the local_path are iterated first. The paths do not end with '/', hence
        /// break the loop if the number of slashes to the right from the offset is greater than 0.
        if (slash_num != 0)
            break;

        result.emplace(std::string(k.begin() + local_path.string().size(), k.end()) + "/");
    }

    /// Files.
    auto it = path_map->map.find(local_path.parent_path());
    if (it != path_map->map.end())
    {
        result.insert(it->second.filenames.begin(), it->second.filenames.end());
    }

    return result;
}

bool MetadataStorageFromPlainRewritableObjectStorage::useSeparateLayoutForMetadata() const
{
    return getMetadataKeyPrefix() != object_storage->getCommonKeyPrefix();
}
}
