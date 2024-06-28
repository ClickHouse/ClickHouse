#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>

#include <unordered_set>
#include <IO/ReadHelpers.h>
#include <IO/SharedThreadPools.h>
#include <IO/S3Common.h>
#include <Common/ErrorCodes.h>
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

/// Use a separate layout for metadata iff:
/// 1. The disk endpoint does not contain objects, OR
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

MetadataStorageFromPlainObjectStorage::PathMap loadPathPrefixMap(const std::string & metadata_key_prefix, ObjectStoragePtr object_storage)
{
    MetadataStorageFromPlainObjectStorage::PathMap result;

    ThreadPool & pool = getIOThreadPool().get();
    ThreadPoolCallbackRunnerLocal<void> runner(pool, "PlainRWMetaLoad");
    std::mutex mutex;

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");

    ReadSettings settings;
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

        runner([remote_metadata_path, path, &object_storage, &result, &mutex, &log, &settings, &metadata_key_prefix]
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
            std::pair<MetadataStorageFromPlainObjectStorage::PathMap::iterator, bool> res;
            {
                std::lock_guard lock(mutex);
                res = result.emplace(local_path, remote_path.parent_path());
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
    LOG_DEBUG(log, "Loaded metadata for {} files, found {} directories", num_files, result.size());

    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::add(metric, result.size());
    return result;
}

void getDirectChildrenOnRewritableDisk(
    const std::string & storage_key,
    const std::string & storage_key_perfix,
    const RelativePathsWithMetadata & remote_paths,
    const std::string & local_path,
    const MetadataStorageFromPlainObjectStorage::PathMap & local_path_prefixes,
    SharedMutex & shared_mutex,
    std::unordered_set<std::string> & result)
{
    using PathMap = MetadataStorageFromPlainObjectStorage::PathMap;

    /// Map remote paths into local subdirectories.
    std::unordered_map<PathMap::mapped_type, PathMap::key_type> remote_to_local_subdir;

    {
        std::shared_lock lock(shared_mutex);
        auto end_it = local_path_prefixes.end();
        for (auto it = local_path_prefixes.lower_bound(local_path); it != end_it; ++it)
        {
            const auto & [k, v] = std::make_tuple(it->first.string(), it->second);
            if (!k.starts_with(local_path))
                break;

            auto slash_num = count(k.begin() + local_path.size(), k.end(), '/');
            if (slash_num != 1)
                continue;

            chassert(k.back() == '/');
            remote_to_local_subdir.emplace(v, std::string(k.begin() + local_path.size(), k.end() - 1));
        }
    }

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
        else
        {
            /// Subdirectories.
            chassert(path.find(storage_key_perfix) == 0);
            auto it = remote_to_local_subdir.find(path.substr(storage_key_perfix.size(), slash_pos - storage_key_perfix.size()));
            /// Mapped subdirectories.
            if (it != remote_to_local_subdir.end())
                result.emplace(it->second);
            /// The remote subdirectory name is the same as the local subdirectory.
            else
                result.emplace(path.substr(child_pos, slash_pos - child_pos));
        }
    }
}

}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_)
    : MetadataStorageFromPlainObjectStorage(object_storage_, storage_path_prefix_)
    , metadata_key_prefix(DB::getMetadataKeyPrefix(object_storage))
    , path_map(std::make_shared<PathMap>(loadPathPrefixMap(metadata_key_prefix, object_storage)))
{
    if (object_storage->isWriteOnce())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MetadataStorageFromPlainRewritableObjectStorage is not compatible with write-once storage '{}'",
            object_storage->getName());

    auto keys_gen = std::make_shared<CommonPathPrefixKeyGenerator>(object_storage->getCommonKeyPrefix(), metadata_mutex, path_map);
    object_storage->setKeysGenerator(keys_gen);
}

MetadataStorageFromPlainRewritableObjectStorage::~MetadataStorageFromPlainRewritableObjectStorage()
{
    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::sub(metric, path_map->size());
}

bool MetadataStorageFromPlainRewritableObjectStorage::exists(const std::string & path) const
{
    if (MetadataStorageFromPlainObjectStorage::exists(path))
        return true;

    if (getMetadataKeyPrefix() != object_storage->getCommonKeyPrefix())
    {
        auto key_prefix = object_storage->generateObjectKeyForPath(path, std::nullopt /* key_prefix */).serialize();
        chassert(key_prefix.starts_with(object_storage->getCommonKeyPrefix()));
        auto metadata_key = std::filesystem::path(getMetadataKeyPrefix()) / key_prefix.substr(object_storage->getCommonKeyPrefix().size());
        return object_storage->existsOrHasAnyChild(metadata_key);
    }

    return false;
}

bool MetadataStorageFromPlainRewritableObjectStorage::isDirectory(const std::string & path) const
{
    if (getMetadataKeyPrefix() != object_storage->getCommonKeyPrefix())
    {
        auto directory = std::filesystem::path(object_storage->generateObjectKeyForPath(path, std::nullopt /* key_prefix */).serialize()) / "";
        chassert(directory.string().starts_with(object_storage->getCommonKeyPrefix()));
        auto metadata_key
            = std::filesystem::path(getMetadataKeyPrefix()) / directory.string().substr(object_storage->getCommonKeyPrefix().size());
        return object_storage->existsOrHasAnyChild(metadata_key);
    }
    else
        return MetadataStorageFromPlainObjectStorage::isDirectory(path);
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    auto key_prefix = object_storage->generateObjectKeyForPath(path, std::nullopt /* key_prefix */).serialize();

    RelativePathsWithMetadata files;
    std::string abs_key = key_prefix;
    if (!abs_key.ends_with('/'))
        abs_key += '/';

    object_storage->listObjects(abs_key, files, 0);

    std::unordered_set<std::string> directories;
    getDirectChildrenOnDisk(abs_key, object_storage->getCommonKeyPrefix(), files, path, directories);
    /// List empty directories that are identified by the `prefix.path` metadata files. This is required to, e.g., remove
    /// metadata along with regular files.
    if (object_storage->getCommonKeyPrefix() != getMetadataKeyPrefix())
    {
        chassert(abs_key.starts_with(object_storage->getCommonKeyPrefix()));
        auto metadata_key = std::filesystem::path(getMetadataKeyPrefix()) / abs_key.substr(object_storage->getCommonKeyPrefix().size());
        RelativePathsWithMetadata metadata_files;
        object_storage->listObjects(metadata_key, metadata_files, 0);
        getDirectChildrenOnDisk(metadata_key, getMetadataKeyPrefix(), metadata_files, path, directories);
    }

    return std::vector<std::string>(std::make_move_iterator(directories.begin()), std::make_move_iterator(directories.end()));
}

void MetadataStorageFromPlainRewritableObjectStorage::getDirectChildrenOnDisk(
    const std::string & storage_key,
    const std::string & storage_key_perfix,
    const RelativePathsWithMetadata & remote_paths,
    const std::string & local_path,
    std::unordered_set<std::string> & result) const
{
    getDirectChildrenOnRewritableDisk(storage_key, storage_key_perfix, remote_paths, local_path, *getPathMap(), metadata_mutex, result);
}

}
