#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>

#include <IO/ReadHelpers.h>
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

MetadataStorageFromPlainObjectStorage::PathMap loadPathPrefixMap(const std::string & root, ObjectStoragePtr object_storage)
{
    MetadataStorageFromPlainObjectStorage::PathMap result;

    RelativePathsWithMetadata files;
    object_storage->listObjects(root, files, 0);
    for (const auto & file : files)
    {
        auto remote_path = std::filesystem::path(file->relative_path);
        if (remote_path.filename() != PREFIX_PATH_FILE_NAME)
            continue;

        StoredObject object{file->relative_path};

        auto read_buf = object_storage->readObject(object);
        String local_path;
        readStringUntilEOF(local_path, *read_buf);

        chassert(remote_path.has_parent_path());
        auto res = result.emplace(local_path, remote_path.parent_path());

        /// This can happen if table replication is enabled, then the same local path is written
        /// in `prefix.path` of each replica.
        /// TODO: should replicated tables (e.g., RMT) be explicitly disallowed?
        if (!res.second)
            LOG_WARNING(
                getLogger("MetadataStorageFromPlainObjectStorage"),
                "The local path '{}' is already mapped to a remote path '{}', ignoring: '{}'",
                local_path,
                res.first->second,
                remote_path.parent_path().string());
    }
    auto metric = object_storage->getMetadataStorageMetrics().directory_map_size;
    CurrentMetrics::add(metric, result.size());
    return result;
}

std::vector<std::string> getDirectChildrenOnRewritableDisk(
    const std::string & storage_key,
    const RelativePathsWithMetadata & remote_paths,
    const std::string & local_path,
    const MetadataStorageFromPlainObjectStorage::PathMap & local_path_prefixes,
    SharedMutex & shared_mutex)
{
    using PathMap = MetadataStorageFromPlainObjectStorage::PathMap;

    std::unordered_set<std::string> duplicates_filter;

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
                duplicates_filter.emplace(std::move(filename));
        }
        else
        {
            /// Subdirectories.
            auto it = remote_to_local_subdir.find(path.substr(0, slash_pos));
            /// Mapped subdirectories.
            if (it != remote_to_local_subdir.end())
                duplicates_filter.emplace(it->second);
            /// The remote subdirectory name is the same as the local subdirectory.
            else
                duplicates_filter.emplace(path.substr(child_pos, slash_pos - child_pos));
        }
    }

    return std::vector<std::string>(std::make_move_iterator(duplicates_filter.begin()), std::make_move_iterator(duplicates_filter.end()));
}

}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_)
    : MetadataStorageFromPlainObjectStorage(object_storage_, storage_path_prefix_)
    , path_map(std::make_shared<PathMap>(loadPathPrefixMap(object_storage->getCommonKeyPrefix(), object_storage)))
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

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::getDirectChildrenOnDisk(
    const std::string & storage_key, const RelativePathsWithMetadata & remote_paths, const std::string & local_path) const
{
    return getDirectChildrenOnRewritableDisk(storage_key, remote_paths, local_path, *getPathMap(), metadata_mutex);
}

}
