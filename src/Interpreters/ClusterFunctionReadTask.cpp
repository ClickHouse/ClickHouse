#include <Interpreters/ClusterFunctionReadTask.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOCOL;
    extern const int LOGICAL_ERROR;
}
namespace Setting
{
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context)
{
    if (!object)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`object` cannot be null");

    if (object->data_lake_metadata.has_value())
        data_lake_metadata = object->data_lake_metadata.value();

    auto iceberg_object_info = std::dynamic_pointer_cast<IcebergDataObjectInfo>(object);

    if (iceberg_object_info)
    {
        data_lake_metadata.position_deletes_objects = iceberg_object_info->position_deletes_objects;
        data_lake_metadata.data_object_file_path_key = iceberg_object_info->data_object_file_path_key;
        data_lake_metadata.sequence_number = iceberg_object_info->sequence_number;
        data_lake_metadata.underlying_format_read_schema_id = iceberg_object_info->underlying_format_read_schema_id;
    }


    const bool send_over_whole_archive = !context->getSettingsRef()[Setting::cluster_function_process_archive_on_multiple_nodes];
    path = send_over_whole_archive ? object->getPathOrPathToArchiveIfArchive() : object->getPath();
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(const std::string & path_)
    : path(path_)
{
}

ObjectInfoPtr ClusterFunctionReadTaskResponse::getObjectInfo() const
{
    if (isEmpty())
        return {};

    auto object = data_lake_metadata.position_deletes_objects.empty()
        ? std::make_shared<ObjectInfo>(path)
        : std::make_shared<IcebergDataObjectInfo>(path);

    if (!data_lake_metadata.position_deletes_objects.empty())
    {
        auto iceberg_object = std::static_pointer_cast<IcebergDataObjectInfo>(object);
        iceberg_object->data_object_file_path_key = data_lake_metadata.data_object_file_path_key;
        iceberg_object->position_deletes_objects = data_lake_metadata.position_deletes_objects;
        iceberg_object->sequence_number = data_lake_metadata.sequence_number;
        iceberg_object->underlying_format_read_schema_id = data_lake_metadata.underlying_format_read_schema_id;
    }

    object->data_lake_metadata = data_lake_metadata;

    return object;
}

void ClusterFunctionReadTaskResponse::serialize(WriteBuffer & out, size_t protocol_version) const
{
    writeVarUInt(protocol_version, out);
    writeStringBinary(path, out);

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        data_lake_metadata.serialize(out);
    }
}

void ClusterFunctionReadTaskResponse::deserialize(ReadBuffer & in)
{
    size_t protocol_version = 0;
    readVarUInt(protocol_version, in);
    if (protocol_version < DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION
        || protocol_version > DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL, "Supported protocol versions are in range [{}, {}], got: {}",
            DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION,
            protocol_version);
    }

    readStringBinary(path, in);
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        data_lake_metadata.deserialize(in, path.empty());
    }
}

}
