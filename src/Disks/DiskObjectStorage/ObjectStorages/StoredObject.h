#pragma once

#include <base/types.h>
#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>

#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <span>

namespace DB
{

/// Object metadata: path, size, path_key_for_cache.
struct StoredObject
{
    /// Sentinel meaning "size is not known yet", set by callers that can't
    /// determine the size up-front (S3 `HEAD` without `Content-Length`, local
    /// `stat` failure). Consumers doing offset arithmetic (`OffsetMap`,
    /// `ReaderExecutor`) must check for it and stream until EOF instead of
    /// treating it as a real file size.
    static constexpr uint64_t UnknownSize = std::numeric_limits<uint64_t>::max();

    String remote_path; /// abs path
    String local_path; /// or equivalent "metadata_path"
    std::optional<size_t> read_source_index;

    /// NOTE: the type must stay uint64_t — MetadataStorageFromDisk removal log serializes it as UInt64 LE.
    uint64_t bytes_size = UnknownSize;

    /// ETag from when the object was listed/headed; the read path rejects a GET whose ETag differs,
    /// catching an in-place overwrite mid-read. Empty means no validation.
    String etag;
    /// Version ID from when the object was read; empty if unversioned or unknown.
    String version_id;

    /// What `cache_token_hash` is computed from.
    enum class CacheTokenKind : UInt8
    {
        /// The ETag of the object. A `GET` response can be checked against it.
        ETag = 0,
        /// `ObjectMetadata::immutable_content_id` of an object known from the records of its table without an ETag
        /// (a data file of Iceberg or Delta Lake). It is not an ETag, so it cannot be checked against a `GET` response.
        ImmutableContentId = 1,
    };

    /// `getCacheTokenHash` of `ObjectMetadata::getContentCacheToken`, 0 if not computed. Used only where a read asks
    /// for it (the key of the distributed cache).
    UInt64 cache_token_hash = 0;
    CacheTokenKind cache_token_kind = CacheTokenKind::ETag;

    explicit StoredObject(
        const String & remote_path_ = "",
        const String & local_path_ = "",
        uint64_t bytes_size_ = UnknownSize,
        std::optional<size_t> read_source_index_ = std::nullopt)
        : remote_path(remote_path_)
        , local_path(local_path_)
        , read_source_index(read_source_index_)
        , bytes_size(bytes_size_)
    {}

    auto operator<=>(const StoredObject & other) const noexcept = default;
};

using StoredObjects = std::vector<StoredObject>;
using StoredObjectSet = std::unordered_set<StoredObject>;
using StoredObjectsSpan = std::span<const StoredObject>;

size_t getTotalSize(const StoredObjects & objects);
Strings collectRemotePaths(const StoredObjects & objects);

UInt64 getCacheTokenHash(const String & token);

}

template <>
struct std::hash<DB::StoredObject>
{
    size_t operator()(const DB::StoredObject & blob) const
    {
        return std::hash<std::string>{}(blob.remote_path);
    }
};
