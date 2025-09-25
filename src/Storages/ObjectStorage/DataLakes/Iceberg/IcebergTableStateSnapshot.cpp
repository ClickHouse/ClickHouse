#include "config.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>

namespace DB
{

using namespace Iceberg;

void TableStateSnapshot::serialize(WriteBuffer & out) const
{
    writeStringBinary(metadata_file_path, out);
    writeIntBinary(metadata_version, out);
    writeIntBinary(schema_id, out);
    writeChar(snapshot_id.has_value() ? 1 : 0, out);
    if (snapshot_id.has_value())
        writeIntBinary(snapshot_id.value(), out);
}

TableStateSnapshot TableStateSnapshot::deserialize(ReadBuffer & in)
{
    TableStateSnapshot state;
    readStringBinary(state.metadata_file_path, in);
    readIntBinary(state.metadata_version, in);
    readIntBinary(state.schema_id, in);
    char snapshot_has_value;
    readChar(snapshot_has_value, in);
    if (snapshot_has_value != 0)
    {
        Int64 snapshot_value;
        readIntBinary(snapshot_value, in);
        state.snapshot_id = snapshot_value;
    }
    else
    {
        state.snapshot_id = std::nullopt;
    }
    return state;
}

bool TableStateSnapshot::operator==(const TableStateSnapshot & other) const
{
    return metadata_file_path == other.metadata_file_path && metadata_version == other.metadata_version && schema_id == other.schema_id
        && snapshot_id == other.snapshot_id;
}
}
