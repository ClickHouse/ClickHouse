#pragma once

#include <functional>
#include <string>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>


namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    /// Absolute path of the blob in object storage.
    std::string absolute_path;
    /// A map which is mapped to current blob (for example, a corresponding local path as clickhouse sees it).
    std::string mapped_path;

    uint64_t bytes_size = 0;

    std::string getPathKeyForCache() const;

    const std::string & getMappedPath() const;

    /// Create `StoredObject` based on metadata storage and blob name of the object.
    static StoredObject create(
        const IObjectStorage & object_storage,
        const std::string & object_path,
        size_t object_size = 0,
        const std::string & mapped_path = "",
        bool exists = false,
        bool object_bypasses_cache = false);

    /// Optional hint for cache. Use delayed initialization
    /// because somecache hint implementation requires it.
    using PathKeyForCacheCreator = std::function<std::string(const std::string &)>;
    PathKeyForCacheCreator path_key_for_cache_creator;

    StoredObject() = default;

    explicit StoredObject(
        const std::string & absolute_path_,
        uint64_t bytes_size_ = 0,
        const std::string & mapped_path_ = "",
        PathKeyForCacheCreator && path_key_for_cache_creator_ = {});
};

using StoredObjects = std::vector<StoredObject>;

}
