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

#if USE_AVRO
    if (std::dynamic_pointer_cast<IcebergDataObjectInfo>(object))
    {
        iceberg_info = dynamic_cast<IcebergDataObjectInfo &>(*object).info;
    }
#endif

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

    ObjectInfoPtr object;

    if (iceberg_info.has_value())
    {
#if USE_AVRO
        auto iceberg_object = std::make_shared<IcebergDataObjectInfo>(RelativePathWithMetadata{path});
        iceberg_object->info = iceberg_info.value();
        object = iceberg_object;
#else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg support is disabled");
#endif
    }
    else
    {
        object = std::make_shared<ObjectInfo>(path);
    }
    object->data_lake_metadata = data_lake_metadata;
    return object;
}

void ClusterFunctionReadTaskResponse::serialize(WriteBuffer & out, size_t worker_protocol_version) const
{
    auto protocol_version
        = std::min(static_cast<UInt64>(worker_protocol_version), static_cast<UInt64>(DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION));
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

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        if (iceberg_info.has_value())
        {
            writeVarUInt(1, out);
            iceberg_info->serializeForClusterFunctionProtocol(out, protocol_version);
        }
        else
        {
            writeVarUInt(0, out);
        }
    }
}

void ClusterFunctionReadTaskResponse::deserialize(ReadBuffer & in)
{
    size_t protocol_version = 0;
    readVarUInt(protocol_version, in);
    if (protocol_version < DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION || protocol_version > DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Supported protocol versions are in range [{}, {}], got: {}",
            DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION,
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION,
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
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        auto has_iceberg_metadata = false;
        readVarUInt(has_iceberg_metadata, in);
        if (has_iceberg_metadata)
        {
            iceberg_info = Iceberg::IcebergObjectSerializableInfo{};
            iceberg_info->deserializeForClusterFunctionProtocol(in, protocol_version);
        }
    }
}

}
