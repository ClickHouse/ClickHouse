#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Common/logger_useful.h>


namespace DB
{
void DataLakeObjectMetadata::serialize(WriteBuffer & out) const
{
    SerializedSetsRegistry registry;
    if (transform)
        transform->serialize(out, registry);
    else
        ActionsDAG().serialize(out, registry);

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

    writeStringBinary(data_object_file_path_key, out);
    writeVarInt(underlying_format_read_schema_id, out);
    writeVarInt(sequence_number, out);
}

void DataLakeObjectMetadata::deserialize(ReadBuffer & in, bool path_empty)
{
    DeserializedSetsRegistry registry;
    auto new_transform = std::make_shared<ActionsDAG>(ActionsDAG::deserialize(in, registry, Context::getGlobalContextInstance()));

    if (!path_empty && !new_transform->getInputs().empty())
    {
        transform = std::move(new_transform);
    }

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
        if (has_reference_path == 1)
        {
            String reference_path;
            readStringBinary(reference_path, in);
            pos_delete_obj.reference_data_file_path = reference_path;
        }
    }
    readStringBinary(data_object_file_path_key, in);
    readVarInt(underlying_format_read_schema_id, in);
    readVarInt(sequence_number, in);
}
}
