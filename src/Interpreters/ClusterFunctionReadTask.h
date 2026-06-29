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
    explicit ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context);

    /// Data path (object path, in case of object storage).
    String path;
    FileBucketInfoPtr file_bucket_info;
    /// Object metadata path, in case of data lake object.
    DataLakeObjectMetadata data_lake_metadata;
    /// Iceberg object metadata
    std::optional<Iceberg::IcebergObjectSerializableInfo> iceberg_info;

    /// Object metadata captured on the coordinator at split/listing time, i.e. the generation it
    /// read to compute the bucket boundaries. It is propagated so the worker pins its read to the
    /// SAME generation via read-time ETag validation, instead of re-fetching metadata and validating
    /// against a possibly newer (concurrently overwritten) generation - which would let stale bucket
    /// offsets be applied to new bytes without `S3_OBJECT_CHANGED_DURING_READ`. An empty `etag` means
    /// "not captured" (e.g. ETag validation disabled, or a path that never read the object); in that
    /// case the worker fetches the metadata itself, as before.
    String etag;
    UInt64 size_bytes = 0;
    bool is_size_known = true;
    UInt64 last_modified_epoch_us = 0;

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
