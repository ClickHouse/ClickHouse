#include "config.h"

#include <Core/Field.h>
#include <Common/FieldVisitorToString.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <Interpreters/Context.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Common/Exception.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_PROTOCOL;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}


using namespace DB::Iceberg;

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_experimental_iceberg_deletion_vectors;
extern const SettingsBool use_roaring_bitmap_iceberg_positional_deletes;
};

namespace Iceberg
{
String computePartitionId(const Row & partition_key_value)
{
    if (partition_key_value.empty())
        return {};
    String result;
    for (const auto & val : partition_key_value)
    {
        if (!result.empty())
            result += '_';
        result += applyVisitor(FieldVisitorToString{}, val);
    }
    return result;
}
}

#if USE_AVRO

IcebergDataObjectInfo::IcebergDataObjectInfo(
    Iceberg::ProcessedManifestFileEntryPtr data_manifest_file_entry_, const String & resolved_storage_path_, Int32 schema_id_relevant_to_iterator_)
    : ObjectInfo(RelativePathWithMetadata(resolved_storage_path_))
    , info{
          data_manifest_file_entry_->parsed_entry->file_path_key,
          data_manifest_file_entry_->resolved_schema_id,
          schema_id_relevant_to_iterator_,
          data_manifest_file_entry_->sequence_number,
          data_manifest_file_entry_->parsed_entry->file_format,
          /* manifest_file */ data_manifest_file_entry_->manifest_file_path,
          /* partition_id */ Iceberg::computePartitionId(data_manifest_file_entry_->parsed_entry->partition_key_value),
          /* position_deletes_objects */ {},
          /* equality_deletes_objects */ {},
          data_manifest_file_entry_->parsed_entry->record_count,
          data_manifest_file_entry_->parsed_entry->file_size_in_bytes}
{
}

IcebergDataObjectInfo::IcebergDataObjectInfo(const RelativePathWithMetadata & path_)
    : ObjectInfo(path_)
{
}

IcebergDataObjectInfo::IcebergDataObjectInfo(const RelativePathWithMetadata & path_, const Iceberg::IcebergObjectSerializableInfo & info_)
    : ObjectInfo(path_)
    , info(info_)
{
}

std::shared_ptr<ISimpleTransform> IcebergDataObjectInfo::getPositionDeleteTransformer(
    ObjectStoragePtr object_storage,
    const SharedHeader & header,
    const std::optional<FormatSettings> & format_settings,
    FormatParserSharedResourcesPtr parser_shared_resources,
    ContextPtr context_)
{
    IcebergDataObjectInfoPtr self = shared_from_this();
    const bool has_deletion_vectors = std::ranges::any_of(
        info.position_deletes_objects,
        [](const Iceberg::PositionDeleteObject & object) { return object.isDeletionVector(); });
    const bool can_read_deletion_vectors = context_->getSettingsRef()[Setting::allow_experimental_iceberg_deletion_vectors].value;
    if (!(has_deletion_vectors && can_read_deletion_vectors)
        && !context_->getSettingsRef()[Setting::use_roaring_bitmap_iceberg_positional_deletes].value)
        return std::make_shared<IcebergStreamingPositionDeleteTransform>(header, self, object_storage, format_settings, parser_shared_resources, context_);
    else
        return std::make_shared<IcebergBitmapPositionDeleteTransform>(header, self, object_storage, format_settings, parser_shared_resources, context_);
}

void IcebergDataObjectInfo::addPositionDeleteObject(Iceberg::ProcessedManifestFileEntryPtr position_delete_object, const String & resolved_storage_path)
{
    const bool is_deletion_vector = Poco::toUpper(position_delete_object->parsed_entry->file_format) == "PUFFIN";
    if (Poco::toUpper(info.file_format) != "PARQUET")
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            info.file_format);
    }
    if (is_deletion_vector)
    {
        if (!position_delete_object->parsed_entry->referenced_data_file_path.has_value())
            throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Iceberg deletion vector does not have referenced_data_file");
        if (!position_delete_object->parsed_entry->content_offset.has_value()
            || !position_delete_object->parsed_entry->content_size_in_bytes.has_value())
            throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Iceberg deletion vector does not have content offset or size");

        info.position_deletes_objects.emplace_back(
            resolved_storage_path,
            position_delete_object->parsed_entry->file_format,
            position_delete_object->parsed_entry->referenced_data_file_path->serialize(),
            position_delete_object->sequence_number,
            Iceberg::PositionDeleteObjectKind::DeletionVector,
            position_delete_object->parsed_entry->content_offset,
            position_delete_object->parsed_entry->content_size_in_bytes);
        return;
    }

    info.position_deletes_objects.emplace_back(
        resolved_storage_path, position_delete_object->parsed_entry->file_format, std::nullopt,
        position_delete_object->sequence_number);
}

void IcebergDataObjectInfo::addEqualityDeleteObject(const Iceberg::ProcessedManifestFileEntryPtr & equality_delete_object, const String & resolved_storage_path)
{
    info.equality_deletes_objects.emplace_back(
        resolved_storage_path,
        equality_delete_object->parsed_entry->file_format,
        equality_delete_object->parsed_entry->equality_ids,
        equality_delete_object->resolved_schema_id);
}

#endif

void IcebergObjectSerializableInfo::serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const
{
    checkVersion(protocol_version);
    writeStringBinary(data_object_file_path_key.serialize(), out);
    writeVarInt(underlying_format_read_schema_id, out);
    writeVarInt(schema_id_relevant_to_iterator, out);
    writeVarInt(sequence_number, out);
    writeStringBinary(file_format, out);
    {
        writeVarUInt(position_deletes_objects.size(), out);
        for (const auto & pos_delete_obj : position_deletes_objects)
        {
            if (pos_delete_obj.isDeletionVector() && protocol_version < DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_DELETION_VECTORS)
                throw Exception(
                    ErrorCodes::UNKNOWN_PROTOCOL,
                    "Iceberg deletion vector serialization is supported since protocol version {}, got: {}",
                    DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_DELETION_VECTORS,
                    protocol_version);
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
            if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_DELETION_VECTORS)
            {
                writeVarUInt(static_cast<UInt8>(pos_delete_obj.kind), out);
                if (pos_delete_obj.content_offset.has_value())
                {
                    writeVarUInt(1, out);
                    writeVarInt(*pos_delete_obj.content_offset, out);
                }
                else
                {
                    writeVarUInt(0, out);
                }
                if (pos_delete_obj.content_size_in_bytes.has_value())
                {
                    writeVarUInt(1, out);
                    writeVarInt(*pos_delete_obj.content_size_in_bytes, out);
                }
                else
                {
                    writeVarUInt(0, out);
                }
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
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_FILE_STATS)
    {
        if (record_count.has_value())
        {
            writeVarUInt(1, out);
            writeVarInt(*record_count, out);
        }
        else
        {
            writeVarUInt(0, out);
        }
        if (file_size_in_bytes.has_value())
        {
            writeVarUInt(1, out);
            writeVarInt(*file_size_in_bytes, out);
        }
        else
        {
            writeVarUInt(0, out);
        }
    }
}

void IcebergObjectSerializableInfo::deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version)
{
    checkVersion(protocol_version);
    {
        String raw_path;
        readStringBinary(raw_path, in);
        data_object_file_path_key = IcebergPathFromMetadata::deserialize(std::move(raw_path));
    }
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
            if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_DELETION_VECTORS)
            {
                UInt64 kind = 0;
                readVarUInt(kind, in);
                pos_delete_obj.kind = static_cast<Iceberg::PositionDeleteObjectKind>(kind);

                size_t has_content_offset = 0;
                readVarUInt(has_content_offset, in);
                if (has_content_offset)
                {
                    Int64 value = 0;
                    readVarInt(value, in);
                    pos_delete_obj.content_offset = value;
                }

                size_t has_content_size = 0;
                readVarUInt(has_content_size, in);
                if (has_content_size)
                {
                    Int64 value = 0;
                    readVarInt(value, in);
                    pos_delete_obj.content_size_in_bytes = value;
                }
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
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_FILE_STATS)
    {
        size_t has_record_count = 0;
        readVarUInt(has_record_count, in);
        if (has_record_count)
        {
            Int64 value = 0;
            readVarInt(value, in);
            record_count = value;
        }
        else
        {
            record_count = std::nullopt;
        }
        size_t has_file_size = 0;
        readVarUInt(has_file_size, in);
        if (has_file_size)
        {
            Int64 value = 0;
            readVarInt(value, in);
            file_size_in_bytes = value;
        }
        else
        {
            file_size_in_bytes = std::nullopt;
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
