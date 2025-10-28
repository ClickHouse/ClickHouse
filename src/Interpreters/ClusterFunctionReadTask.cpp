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

static IcebergObjectSerializableInfo extractIcebergMetadata(const IcebergDataObjectInfo & object)
{
    IcebergObjectSerializableInfo info;
    info.data_object_file_path_key = object.data_object_file_path_key;
    info.underlying_format_read_schema_id = object.underlying_format_read_schema_id;
    info.schema_id_relevant_to_iterator = object.schema_id_relevant_to_iterator;
    info.position_deletes_objects = object.position_deletes_objects;
    info.equality_deletes_objects = object.equality_deletes_objects;
    info.sequence_number = object.sequence_number;
    info.file_format = object.file_format;
    return info;
}

static IcebergObjectSerializableInfo setIcebergMetadata(IcebergDataObjectInfo & object, const IcebergObjectSerializableInfo & info)
{
    object.data_object_file_path_key = info.data_object_file_path_key;
    object.underlying_format_read_schema_id = info.underlying_format_read_schema_id;
    object.schema_id_relevant_to_iterator = info.schema_id_relevant_to_iterator;
    object.position_deletes_objects = info.position_deletes_objects;
    object.equality_deletes_objects = info.equality_deletes_objects;
    object.sequence_number = info.sequence_number;
    object.file_format = info.file_format;
    return info;
}


ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context)
{
    if (!object)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`object` cannot be null");

    if (object->data_lake_metadata.has_value())
        data_lake_metadata = object->data_lake_metadata.value();

    if (std::dynamic_pointer_cast<IcebergDataObjectInfo>(object))
    {
        iceberg_info = extractIcebergMetadata(dynamic_cast<IcebergDataObjectInfo &>(*object));
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

    ObjectInfoPtr object;

    if (iceberg_info.has_value())
    {
        auto iceberg_object = std::make_shared<IcebergDataObjectInfo>(path);
        setIcebergMetadata(*iceberg_object, iceberg_info.value());
        object = iceberg_object;
    }
    else
    {
        object = std::make_shared<ObjectInfo>(path);
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
            iceberg_info->serialize(out, protocol_version);
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
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        auto has_iceberg_metadata = false;
        readVarUInt(has_iceberg_metadata, in);
        if (has_iceberg_metadata)
        {
            iceberg_info = IcebergObjectSerializableInfo{};
            iceberg_info->deserialize(in, protocol_version);
        }
    }
}

void IcebergObjectSerializableInfo::checkVersion(size_t protocol_version) const
{
    if (protocol_version < DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "IcebergObjectSerializableInfo serialization is supported since protocol version {}, got: {}",
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA,
            protocol_version);
    }
}

void IcebergObjectSerializableInfo::serialize(WriteBuffer & out, size_t protocol_version) const
{
    checkVersion(protocol_version);
    writeStringBinary(data_object_file_path_key, out);
    writeVarInt(underlying_format_read_schema_id, out);
    writeVarInt(schema_id_relevant_to_iterator, out);
    writeVarInt(sequence_number, out);
    writeStringBinary(file_format, out);
    {
        writeVarUInt(position_deletes_objects.size(), out);
        for (const auto & pos_delete_obj : position_deletes_objects)
        {
            writeStringBinary(pos_delete_obj.file_path, out);
            writeStringBinary(pos_delete_obj.file_format, out);
            if (pos_delete_obj.reference_data_file_path.has_value())
            {
                writeVarUInt(1, out);
                writeStringBinary(pos_delete_obj.reference_data_file_path.value(), out);
            }
            else
            {
                writeVarUInt(0, out);
            }
        }
    }
    {
        writeVarUInt(equality_deletes_objects.size(), out);
        for (const auto & eq_delete_obj : equality_deletes_objects)
        {
            writeStringBinary(eq_delete_obj.file_path, out);
            writeStringBinary(eq_delete_obj.file_format, out);
            writeVarInt(eq_delete_obj.schema_id, out);
            if (!eq_delete_obj.equality_ids)
            {
                writeVarUInt(0, out);
            }
            else
            {
                writeVarUInt(1, out);
                writeVarUInt(eq_delete_obj.equality_ids->size(), out);
                for (const auto & equality_id : *eq_delete_obj.equality_ids)
                {
                    writeVarInt(equality_id, out);
                }
            }
        }
    }
}

void IcebergObjectSerializableInfo::deserialize(ReadBuffer & in, size_t protocol_version)
{
    checkVersion(protocol_version);
    if (protocol_version < DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "IcebergObjectSerializableInfo deserialization is supported since protocol version {}, got: {}",
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA,
            protocol_version);
    }
    readStringBinary(data_object_file_path_key, in);
    readVarInt(underlying_format_read_schema_id, in);
    readVarInt(schema_id_relevant_to_iterator, in);
    readVarInt(sequence_number, in);
    readStringBinary(file_format, in);

    {
        size_t pos_delete_obj_size = 0;
        readVarUInt(pos_delete_obj_size, in);
        position_deletes_objects.resize(pos_delete_obj_size);
        for (size_t i = 0; i < pos_delete_obj_size; ++i)
        {
            Iceberg::PositionDeleteObject & pos_delete_obj = position_deletes_objects[i];
            readStringBinary(pos_delete_obj.file_path, in);
            readStringBinary(pos_delete_obj.file_format, in);
            size_t has_reference_path = 0;
            readVarUInt(has_reference_path, in);
            if (has_reference_path)
            {
                String reference_path;
                readStringBinary(reference_path, in);
                pos_delete_obj.reference_data_file_path = reference_path;
            }
        }
    }
    {
        size_t eq_delete_obj_size = 0;
        readVarUInt(eq_delete_obj_size, in);
        equality_deletes_objects.resize(eq_delete_obj_size);
        for (size_t i = 0; i < eq_delete_obj_size; ++i)
        {
            Iceberg::EqualityDeleteObject & eq_delete_obj = equality_deletes_objects[i];
            readStringBinary(eq_delete_obj.file_path, in);
            readStringBinary(eq_delete_obj.file_format, in);
            readVarInt(eq_delete_obj.schema_id, in);
            size_t has_equality_ids = 0;
            readVarUInt(has_equality_ids, in);
            if (has_equality_ids)
            {
                size_t equality_ids_size = 0;
                readVarUInt(equality_ids_size, in);
                eq_delete_obj.equality_ids = std::vector<Int32>{};
                for (size_t j = 0; j < equality_ids_size; ++j)
                {
                    Int32 equality_id = 0;
                    readVarInt(equality_id, in);
                    eq_delete_obj.equality_ids->push_back(equality_id);
                }
            }
        }
    }
}
}
