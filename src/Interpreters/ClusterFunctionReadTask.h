#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>


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
    /// Object metadata path, in case of data lake object.
    DataLakeObjectMetadata data_lake_metadata;

    /// Convert received response into ObjectInfo.
    ObjectInfoPtr getObjectInfo() const;

    /// Whether response is empty.
    /// It is used to identify an end of processing.
    bool isEmpty() const { return path.empty(); }

    /// Serialize according to the protocol version.
    void serialize(WriteBuffer & out, size_t protocol_version) const;
    /// Deserialize. Protocol version will be received from `in`
    /// and the result will be deserialized accordingly.
    void deserialize(ReadBuffer & in);
};

using ClusterFunctionReadTaskResponsePtr = std::shared_ptr<ClusterFunctionReadTaskResponse>;
using ClusterFunctionReadTaskCallback = std::function<ClusterFunctionReadTaskResponsePtr()>;

}
