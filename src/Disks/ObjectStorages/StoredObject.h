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

    const std::string & getMappedPath() const;

    StoredObject() = default;

    explicit StoredObject(
        const std::string & absolute_path_,
        uint64_t bytes_size_ = 0,
        const std::string & mapped_path_ = "");
};

using StoredObjects = std::vector<StoredObject>;

}
