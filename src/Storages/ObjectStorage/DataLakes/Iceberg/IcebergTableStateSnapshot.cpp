#include "config.h"

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace DB
{

using namespace Iceberg;

void TableStateSnapshot::serialize(WriteBuffer & out) const
{
    writeStringBinary(metadata_file_path, out);
    writeVarInt(metadata_version, out);
    writeVarInt(schema_id, out);
    writeChar(snapshot_id.has_value() ? 1 : 0, out);
    if (snapshot_id.has_value())
        writeVarInt(snapshot_id.value(), out);
}

TableStateSnapshot TableStateSnapshot::deserialize(ReadBuffer & in, const int datalake_state_protocol_version)
{
    if (datalake_state_protocol_version <= 0 || datalake_state_protocol_version > DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot serialize Iceberg::TableStateSnapshot with protocol version {}, maximum supported version is {}",
            datalake_state_protocol_version,
            DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);
    if (datalake_state_protocol_version == 1)
    {
        TableStateSnapshot state;
        readStringBinary(state.metadata_file_path, in);
        readVarInt(state.metadata_version, in);
        readVarInt(state.schema_id, in);
        char snapshot_has_value;
        readChar(snapshot_has_value, in);
        if (snapshot_has_value != 0)
        {
            Int64 snapshot_value;
            readVarInt(snapshot_value, in);
            state.snapshot_id = snapshot_value;
        }
        else
        {
            state.snapshot_id = std::nullopt;
        }
        return state;
    }
    UNREACHABLE();
}

bool TableStateSnapshot::operator==(const TableStateSnapshot & other) const
{
    return metadata_file_path == other.metadata_file_path && metadata_version == other.metadata_version && schema_id == other.schema_id
        && snapshot_id == other.snapshot_id;
}
}
