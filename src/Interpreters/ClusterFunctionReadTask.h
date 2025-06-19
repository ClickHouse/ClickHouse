#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include "Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h"


namespace DB
{
class ReadBuffer;
class WriteBuffer;

struct ClusterFunctionReadTaskResponse
{
    ClusterFunctionReadTaskResponse() = default;
    explicit ClusterFunctionReadTaskResponse(const std::string & path_);
    explicit ClusterFunctionReadTaskResponse(ObjectInfoPtr object);

    String path;
    DataLakeObjectMetadata data_lake_metadata;
    bool is_empty = true;

    ObjectInfoPtr getObjectInfo() const;

    void serialize(WriteBuffer & out, size_t protocol_version) const;
    void deserialize(ReadBuffer & in);
};

using ClusterFunctionReadTaskResponsePtr = std::shared_ptr<ClusterFunctionReadTaskResponse>;
using ClusterFunctionReadTaskCallback = std::function<ClusterFunctionReadTaskResponsePtr()>;

}
