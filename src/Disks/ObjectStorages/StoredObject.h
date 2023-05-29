#pragma once

#include <functional>
#include <string>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>


namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    std::string remote_path;
    std::string local_path; /// or equivalent "metadata_path"

    uint64_t bytes_size = 0;

    StoredObject() = default;

    explicit StoredObject(
        const std::string & remote_path_,
        uint64_t bytes_size_ = 0,
        const std::string & local_path_ = "")
    : remote_path(remote_path_)
    , local_path(local_path_)
    , bytes_size(bytes_size_) {}
};

using StoredObjects = std::vector<StoredObject>;

size_t getTotalSize(const StoredObjects & objects);

}
