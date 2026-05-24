#pragma once

#include <base/types.h>
#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>

#include <functional>
#include <limits>
#include <string>
#include <span>

namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    /// Sentinel meaning "size is not known yet" — used by callers that
    /// can't determine the size up-front (S3 `HEAD` without
    /// `Content-Length`, local `stat()` failure). Consumers that need the
    /// size for offset arithmetic (`OffsetMap`, `ReaderExecutor`) MUST
    /// check for this value and switch to streaming-until-EOF behaviour
    /// instead of treating it as a real file size.
    static constexpr uint64_t UnknownSize = std::numeric_limits<uint64_t>::max();

    String remote_path; /// abs path
    String local_path; /// or equivalent "metadata_path"

    /// NOTE: the type must stay uint64_t — MetadataStorageFromDisk removal log serializes it as UInt64 LE.
    uint64_t bytes_size = UnknownSize;

    explicit StoredObject(
        const String & remote_path_ = "",
        const String & local_path_ = "",
        uint64_t bytes_size_ = UnknownSize)
        : remote_path(remote_path_)
        , local_path(local_path_)
        , bytes_size(bytes_size_)
    {}

    auto operator<=>(const StoredObject & other) const noexcept = default;
};

using StoredObjects = std::vector<StoredObject>;
using StoredObjectSet = std::unordered_set<StoredObject>;
using StoredObjectsSpan = std::span<const StoredObject>;

size_t getTotalSize(const StoredObjects & objects);
Strings collectRemotePaths(const StoredObjects & objects);

}

template <>
struct std::hash<DB::StoredObject>
{
    size_t operator()(const DB::StoredObject & blob) const
    {
        return std::hash<std::string>{}(blob.remote_path);
    }
};
