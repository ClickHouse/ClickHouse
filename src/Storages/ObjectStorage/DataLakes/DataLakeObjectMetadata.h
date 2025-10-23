#pragma once
#include <Interpreters/ActionsDAG.h>
#include <Core/Field.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>

namespace DB
{

struct DataLakeObjectMetadata
{
    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in, bool path_empty);

    std::shared_ptr<ActionsDAG> transform;
    /// Delete objections for iceberg position delete
    std::vector<Iceberg::PositionDeleteObject> position_deletes_objects;
    /// Data object file path key, for iceberg data object
    String data_object_file_path_key;
    Int32 underlying_format_read_schema_id;
    Int64 sequence_number;

};

}
