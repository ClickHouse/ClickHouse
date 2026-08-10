#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/IObjectIterator.h>


namespace DB
{
class ReadBuffer;
class WriteBuffer;

/// A response send from initiator in Cluster functions (S3Cluster, etc)
struct ClusterFunctionReadTaskResponse
{
    ClusterFunctionReadTaskResponse() = default;
    explicit ClusterFunctionReadTaskResponse(const std::string & path_);
    ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context, bool read_pins_generation_ = false);

    /// Data path (object path, in case of object storage).
    String path;
    std::optional<size_t> read_source_index;
    FileBucketInfoPtr file_bucket_info;
    /// Object metadata path, in case of data lake object.
    DataLakeObjectMetadata data_lake_metadata;
    /// Iceberg object metadata
    std::optional<Iceberg::IcebergObjectSerializableInfo> iceberg_info;

    /// Object metadata (notably the ETag) the coordinator already has - from its listing
    /// (`ListObjectsV2` carries the ETag/size) or, on the bucket-split path, refreshed to the generation
    /// it read to compute bucket boundaries. Propagated so the worker skips its own metadata HEAD and,
    /// when `s3_validate_etag_on_read` is enabled, pins its read to that SAME generation via read-time
    /// ETag validation instead of a possibly newer (overwritten) one - which for a bucket-split read would
    /// otherwise apply stale bucket offsets to new bytes without `S3_OBJECT_CHANGED_DURING_READ`.
    /// `has_object_metadata` marks whether the coordinator had any metadata at all (an ETag-less backend
    /// like HDFS still propagates size/time); when false the worker fetches the metadata itself, as before.
    bool has_object_metadata = false;
    String etag;
    UInt64 size_bytes = 0;
    bool is_size_known = true;
    UInt64 last_modified_epoch_us = 0;
    bool is_last_modified_known = true;

    /// Coordinator-side only (NOT serialized): whether this backend's read actually pins to the
    /// propagated ETag generation (only S3 enforces `StoredObject::etag` on the GET via `If-Match`).
    /// Gates the old-worker fail-close in `serialize` — a backend that never pins (Azure, HDFS, ...)
    /// loses no generation-pinning semantics on a `< 9` worker, so it must not be rejected.
    bool read_pins_generation = false;

    /// Convert received response into ObjectInfo.
    ObjectInfoPtr getObjectInfo() const;

    /// Whether response is empty.
    /// It is used to identify an end of processing.
    bool isEmpty() const { return path.empty(); }

    /// Serialize according to the protocol version.
    void serialize(WriteBuffer & out, size_t worker_protocol_version) const;
    /// Deserialize. Protocol version will be received from `in`
    /// and the result will be deserialized accordingly.
    void deserialize(ReadBuffer & in);
};

using ClusterFunctionReadTaskResponsePtr = std::shared_ptr<ClusterFunctionReadTaskResponse>;
using ClusterFunctionReadTaskCallback = std::function<ClusterFunctionReadTaskResponsePtr()>;

}
