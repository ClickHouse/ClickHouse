#pragma once
#include <Core/Types.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <memory>


namespace DB
{
class ReadBuffer;
class WriteBuffer;

/// Forward-declared to keep this header free of `DataLakes/` includes \u2014 the bodies
/// of these types live behind the Lake firewall. Real definitions are pulled in by
/// `ClusterFunctionReadTask.cpp`.
struct DataLakeObjectMetadata;
namespace Iceberg
{
    struct IcebergObjectSerializableInfo;
}

/// A response send from initiator in Cluster functions (S3Cluster, etc)
struct ClusterFunctionReadTaskResponse
{
    ClusterFunctionReadTaskResponse();
    explicit ClusterFunctionReadTaskResponse(const std::string & path_);
    explicit ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context);

    /// User-defined to support PIMPL members below \u2014 the destructor needs the
    /// forward-declared types to be complete, so it lives in the `.cpp`.
    ~ClusterFunctionReadTaskResponse();
    ClusterFunctionReadTaskResponse(const ClusterFunctionReadTaskResponse & other);
    ClusterFunctionReadTaskResponse(ClusterFunctionReadTaskResponse && other) noexcept;
    ClusterFunctionReadTaskResponse & operator=(const ClusterFunctionReadTaskResponse & other);
    ClusterFunctionReadTaskResponse & operator=(ClusterFunctionReadTaskResponse && other) noexcept;

    /// Data path (object path, in case of object storage).
    String path;
    FileBucketInfoPtr file_bucket_info;
    /// Object metadata path, in case of data lake object. Held by `unique_ptr` so this
    /// header does not need to include `DataLakes/DataLakeObjectMetadata.h`. The pointer
    /// is `nullptr` when there is no data-lake metadata (the previous code used a value
    /// type with an `excluded_rows == nullptr` / `schema_transform == nullptr` sentinel).
    std::unique_ptr<DataLakeObjectMetadata> data_lake_metadata;
    /// Iceberg object metadata \u2014 held by `unique_ptr` (formerly `std::optional`) to
    /// keep this header free of `Iceberg/IcebergDataObjectInfo.h`. `nullptr` corresponds
    /// to the absent state.
    std::unique_ptr<Iceberg::IcebergObjectSerializableInfo> iceberg_info;

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
