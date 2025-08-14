#pragma once

#include <base/types.h>

#include <Disks/ObjectStorages/IObjectStorage_fwd.h>

#include <functional>
#include <limits>
#include <string>


namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    String remote_path; /// abs path
    String local_path; /// or equivalent "metadata_path"

    uint64_t bytes_size = std::numeric_limits<uint64_t>::max();

    explicit StoredObject(
        const String & remote_path_ = "",
        const String & local_path_ = "",
        uint64_t bytes_size_ = 0)
        : remote_path(remote_path_)
        , local_path(local_path_)
        , bytes_size(bytes_size_)
    {}
};

using StoredObjects = std::vector<StoredObject>;

size_t getTotalSize(const StoredObjects & objects);

}
