#pragma once

#include <base/types.h>

#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <fmt/core.h>

#include <functional>
#include <string>


namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    String remote_path; /// abs path
    String local_path; /// or equivalent "metadata_path"
    uint64_t bytes_size = 0;

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

template<>
struct fmt::formatter<DB::StoredObject>
{
    constexpr auto parse(auto & ctx) { return ctx.begin(); }
    constexpr auto format(const DB::StoredObject & obj, auto & ctx)
    {
        return fmt::format_to(ctx.out(), "StoredObject({}, local={})", obj.remote_path, obj.local_path);
    }
};
