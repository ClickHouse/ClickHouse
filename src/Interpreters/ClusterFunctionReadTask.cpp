#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ClusterFunctionReadTask.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectInfo.h>
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

    auto * iceberg_object_info = dynamic_cast<IcebergDataObjectInfo *>(object.get());
    if (iceberg_object_info)
    {
        is_iceberg_object = 1;
        data_object_file_path_key = iceberg_object_info->data_object_file_path_key;
        read_schema_id = iceberg_object_info->read_schema_id;
        position_deletes_objects_range = iceberg_object_info->position_deletes_objects_range;
    }

    auto * data_lake_object_info = dynamic_cast<ObjectInfoDataLake *>(object.get());
    if (data_lake_object_info && data_lake_object_info->getDataLakeMetadata().has_value())
    {
        data_lake_metadata = data_lake_object_info->getDataLakeMetadata().value();
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

    if (is_iceberg_object)
    {
        return std::make_shared<IcebergDataObjectInfo>(path, data_object_file_path_key, read_schema_id, position_deletes_objects_range);
    }
    else
    {
        if (data_lake_metadata.transform)
        {
            auto object = std::make_shared<ObjectInfoDataLake>(path);
            object->setDataLakeMetadata(data_lake_metadata);
            return object;
        }
        else
        {
            return std::make_shared<ObjectInfoPlain>(path);
        }
    }
}

void ClusterFunctionReadTaskResponse::serialize(WriteBuffer & out, size_t protocol_version) const
{
    writeVarUInt(protocol_version, out);
    writeStringBinary(path, out);

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        SerializedSetsRegistry registry;
        if (data_lake_metadata.transform)
            data_lake_metadata.transform->serialize(out, registry);
        else
            ActionsDAG().serialize(out, registry);
    }
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_ICEBERG_SPECIFIC)
    {
        writeVarUInt(is_iceberg_object, out);
        if (is_iceberg_object)
        {
            writeVarInt(read_schema_id, out);
            writeVarUInt(position_deletes_objects_range.first, out);
            writeVarUInt(position_deletes_objects_range.second, out);
            writeStringBinary(data_object_file_path_key, out);
        }
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
        DeserializedSetsRegistry registry;
        auto transform = std::make_shared<ActionsDAG>(ActionsDAG::deserialize(in, registry, Context::getGlobalContextInstance()));

        if (!path.empty() && !transform->getInputs().empty())
        {
            data_lake_metadata.transform = std::move(transform);
        }
    }
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_ICEBERG_SPECIFIC)
    {
        readVarUInt(is_iceberg_object, in);
        if (is_iceberg_object)
        {
            readVarInt(read_schema_id, in);
            readVarUInt(position_deletes_objects_range.first, in);
            readVarUInt(position_deletes_objects_range.second, in);
            readStringBinary(data_object_file_path_key, in);
        }
    }
}

}
