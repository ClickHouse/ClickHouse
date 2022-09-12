#pragma once

#include <string>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>

namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    std::string absolute_path;

    uint64_t bytes_size;

    std::string getPathKeyForCache() const;

    /// Create `StoredObject` based on metadata storage and blob name of the object.
    static StoredObject create(
        const IObjectStorage & object_storage,
        const std::string & object_path,
        size_t object_size = 0,
        bool object_bypasses_cache = false);

    /// Optional hint for cache. Use delayed initialization
    /// because somecache hint implementation requires it.
    using PathKeyForCacheCreator = std::function<std::string(const std::string &)>;
    PathKeyForCacheCreator path_key_for_cache_creator;

    explicit StoredObject(
        const std::string & absolute_path_,
        uint64_t bytes_size_ = 0,
        PathKeyForCacheCreator && path_key_for_cache_creator_ = {});
};

}
