#include <Disks/ObjectStorages/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/ObjectStorages/InMemoryPathMap.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>

#include <unordered_set>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/SharedThreadPools.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Common/logger_useful.h>
#include "CommonPathPrefixKeyGenerator.h"


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

std::shared_ptr<InMemoryPathMap> loadPathPrefixMap(const std::string & metadata_key_prefix, ObjectStoragePtr object_storage)
{
    auto result = std::make_shared<InMemoryPathMap>();
    using Map = InMemoryPathMap::Map;

    ThreadPool & pool = getIOThreadPool().get();
    ThreadPoolCallbackRunnerLocal<void> runner(pool, "PlainRWMetaLoad");

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.remote_fs_method = RemoteFSReadMethod::read;
    settings.remote_fs_buffer_size = 1024;  /// These files are small.

    LOG_DEBUG(log, "Loading metadata");
    size_t num_files = 0;
    for (auto iterator = object_storage->iterate(metadata_key_prefix, 0); iterator->isValid(); iterator->next())
    {
        ++num_files;
        auto file = iterator->current();
        String path = file->getPath();
        auto remote_metadata_path = std::filesystem::path(path);
        if (remote_metadata_path.filename() != PREFIX_PATH_FILE_NAME)
            continue;

        runner(
            [remote_metadata_path, path, &object_storage, &result, &log, &settings, &metadata_key_prefix]
            {
                setThreadName("PlainRWMetaLoad");

                StoredObject object{path};
                String local_path;

                try
                {
                    auto read_buf = object_storage->readObject(object, settings);
                    readStringUntilEOF(local_path, *read_buf);
                }
#if USE_AWS_S3
                catch (const S3Exception & e)
                {
                    /// It is ok if a directory was removed just now.
                    /// We support attaching a filesystem that is concurrently modified by someone else.
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
                auto remote_path = std::filesystem::path(std::move(suffix));
                std::pair<Map::iterator, bool> res;
                {
                    std::lock_guard lock(result->mutex);
                    res = result->map.emplace(std::filesystem::path(local_path).parent_path(), remote_path.parent_path());
                }

                /// This can happen if table replication is enabled, then the same local path is written
                /// in `prefix.path` of each replica.
                /// TODO: should replicated tables (e.g., RMT) be explicitly disallowed?
                if (!res.second)
                    LOG_WARNING(
                        log,
                        "The local path '{}' is already mapped to a remote path '{}', ignoring: '{}'",
                        local_path,
                        res.first->second,
                        remote_path.parent_path().string());
            });
    }

    runner.waitForAllToFinishAndRethrowFirstError();
    {
        SharedLockGuard lock(result->mutex);
        LOG_DEBUG(log, "Loaded metadata for {} files, found {} directories", num_files, result->map.size());

        auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
        CurrentMetrics::add(metric, result->map.size());
    }
    return result;
}

void getDirectChildrenOnDiskImpl(
    const std::string & storage_key,
    const RelativePathsWithMetadata & remote_paths,
    const std::string & local_path,
    const InMemoryPathMap & path_map,
    std::unordered_set<std::string> & result)
{
    /// Directories are retrieved from the in-memory path map.
    {
        SharedLockGuard lock(path_map.mutex);
        const auto & local_path_prefixes = path_map.map;
        const auto end_it = local_path_prefixes.end();
        for (auto it = local_path_prefixes.lower_bound(local_path); it != end_it; ++it)
        {
            const auto & [k, _] = std::make_tuple(it->first.string(), it->second);
            if (!k.starts_with(local_path))
                break;

            auto slash_num = count(k.begin() + local_path.size(), k.end(), '/');
            /// The local_path_prefixes comparator ensures that the paths with the smallest number of
            /// hops from the local_path are iterated first. The paths do not end with '/', hence
            /// break the loop if the number of slashes is greater than 0.
            if (slash_num != 0)
                break;

            result.emplace(std::string(k.begin() + local_path.size(), k.end()) + "/");
        }
    }

    /// Files.
    auto skip_list = std::set<std::string>{PREFIX_PATH_FILE_NAME};
    for (const auto & elem : remote_paths)
    {
        const auto & path = elem->relative_path;
        chassert(path.find(storage_key) == 0);
        const auto child_pos = storage_key.size();

        auto slash_pos = path.find('/', child_pos);

        if (slash_pos == std::string::npos)
        {
            /// File names.
            auto filename = path.substr(child_pos);
            if (!skip_list.contains(filename))
                result.emplace(std::move(filename));
        }
    }
}

}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t file_sizes_cache_size)
    : MetadataStorageFromPlainObjectStorage(object_storage_, storage_path_prefix_, file_sizes_cache_size)
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

    ObjectStorageKey object_key = object_storage->generateObjectKeyForPath(path, std::nullopt /* key_prefix */);
    StoredObject object(object_key.serialize(), path);
    return object_storage->exists(object);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    if (existsDirectory(path))
        return false;

    ObjectStorageKey object_key = object_storage->generateObjectKeyForPath(path, std::nullopt /* key_prefix */);
    StoredObject object(object_key.serialize(), path);
    return object_storage->exists(object);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsDirectory(const std::string & path) const
{
    auto base_path = path;
    if (base_path.ends_with('/'))
        base_path.pop_back();

    SharedLockGuard lock(path_map->mutex);
    return path_map->map.find(base_path) != path_map->map.end();
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    auto key_prefix = object_storage->generateObjectKeyForPath(path, "" /* key_prefix */).serialize();

    RelativePathsWithMetadata files;
    auto absolute_key = std::filesystem::path(object_storage->getCommonKeyPrefix()) / key_prefix / "";

    object_storage->listObjects(absolute_key, files, 0);

    std::unordered_set<std::string> directories;
    getDirectChildrenOnDisk(absolute_key, files, std::filesystem::path(path) / "", directories);

    return std::vector<std::string>(std::make_move_iterator(directories.begin()), std::make_move_iterator(directories.end()));
}

void MetadataStorageFromPlainRewritableObjectStorage::getDirectChildrenOnDisk(
    const std::string & storage_key,
    const RelativePathsWithMetadata & remote_paths,
    const std::string & local_path,
    std::unordered_set<std::string> & result) const
{
    getDirectChildrenOnDiskImpl(storage_key, remote_paths, local_path, *getPathMap(), result);
}

bool MetadataStorageFromPlainRewritableObjectStorage::useSeparateLayoutForMetadata() const
{
    return getMetadataKeyPrefix() != object_storage->getCommonKeyPrefix();
}
}
