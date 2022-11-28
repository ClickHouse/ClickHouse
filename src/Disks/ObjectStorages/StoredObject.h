#pragma once

#include <string>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>


namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    std::string absolute_path;
    std::string connected_path;

    uint64_t bytes_size = 0;

    std::string getPathKeyForCache() const;

    const std::string & getConnectedPath() const;

    /// Create `StoredObject` based on metadata storage and blob name of the object.
    static StoredObject create(
        const IObjectStorage & object_storage,
        const std::string & object_path,
        const std::string & connected_path = "",
        size_t object_size = 0,
        bool exists = false,
        bool object_bypasses_cache = false);

    /// Optional hint for cache. Use delayed initialization
    /// because somecache hint implementation requires it.
    using PathKeyForCacheCreator = std::function<std::string(const std::string &)>;
    PathKeyForCacheCreator path_key_for_cache_creator;

    StoredObject(
        const std::string & absolute_path_,
        const std::string & connected_path_ = "",
        uint64_t bytes_size_ = 0,
        PathKeyForCacheCreator && path_key_for_cache_creator_ = {});
};

using StoredObjects = std::vector<StoredObject>;

}
