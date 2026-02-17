#include "config.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <base/defines.h>
#include <Common/SharedMutex.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_PROTOCOL;
}


using namespace DB::Iceberg;

namespace DB
{

namespace Setting
{
extern const SettingsBool use_roaring_bitmap_iceberg_positional_deletes;
};

#if USE_AVRO

IcebergDataObjectInfo::IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_, Int32 schema_id_relevant_to_iterator_)
    : ObjectInfo(RelativePathWithMetadata(data_manifest_file_entry_.file_path))
    , info{
          data_manifest_file_entry_.file_path_key,
          data_manifest_file_entry_.schema_id,
          schema_id_relevant_to_iterator_,
          data_manifest_file_entry_.added_sequence_number,
          data_manifest_file_entry_.file_format,
          /* position_deletes_objects */ {},
          /* equality_deletes_objects */ {}}
{
}

IcebergDataObjectInfo::IcebergDataObjectInfo(const RelativePathWithMetadata & path_)
    : ObjectInfo(path_)
{
}

std::shared_ptr<ISimpleTransform> IcebergDataObjectInfo::getPositionDeleteTransformer(
    ObjectStoragePtr object_storage,
    const SharedHeader & header,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context_)
{
    IcebergDataObjectInfoPtr self = shared_from_this();
    if (!context_->getSettingsRef()[Setting::use_roaring_bitmap_iceberg_positional_deletes].value)
        return std::make_shared<IcebergStreamingPositionDeleteTransform>(header, self, object_storage, format_settings, context_);
    else
        return std::make_shared<IcebergBitmapPositionDeleteTransform>(header, self, object_storage, format_settings, context_);
}

void IcebergDataObjectInfo::addPositionDeleteObject(Iceberg::ManifestFileEntry position_delete_object)
{
    if (Poco::toUpper(info.file_format) != "PARQUET")
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            info.file_format);
    }
    info.position_deletes_objects.emplace_back(
        position_delete_object.file_path, position_delete_object.file_format, position_delete_object.reference_data_file_path);
}

void IcebergDataObjectInfo::addEqualityDeleteObject(const Iceberg::ManifestFileEntry & equality_delete_object)
{
    info.equality_deletes_objects.emplace_back(
        equality_delete_object.file_path,
        equality_delete_object.file_format,
        equality_delete_object.equality_ids,
        equality_delete_object.schema_id);
}

#endif

void IcebergObjectSerializableInfo::serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const
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
            if (eq_delete_obj.equality_ids.has_value())
            {
                writeVarUInt(1, out);
                writeVarUInt(eq_delete_obj.equality_ids->size(), out);
                for (const auto & equality_id : *eq_delete_obj.equality_ids)
                {
                    writeVarInt(equality_id, out);
                }
            }
            else
            {
                writeVarUInt(0, out);
            }
        }
    }
}

void IcebergObjectSerializableInfo::deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version)
{
    checkVersion(protocol_version);
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
}

