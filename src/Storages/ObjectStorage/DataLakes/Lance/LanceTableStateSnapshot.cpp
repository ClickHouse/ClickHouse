#include "config.h"

#if USE_LANCE

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Lance
{

void TableStateSnapshot::serialize(WriteBuffer & out) const
{
    writeVarInt(snapshot_id, out);
    writeVarInt(schema_id, out);
}

TableStateSnapshot TableStateSnapshot::deserialize(ReadBuffer & in, int datalake_state_protocol_version)
{
    if (datalake_state_protocol_version <= 0 || datalake_state_protocol_version > DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot deserialize Lance::TableStateSnapshot with protocol version {}, maximum supported version is {}",
            datalake_state_protocol_version,
            DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);

    TableStateSnapshot state;
    Int64 snapshot_id = 0;
    Int64 schema_id = 0;
    readVarInt(snapshot_id, in);
    readVarInt(schema_id, in);
    state.snapshot_id = snapshot_id;
    state.schema_id = schema_id;
    return state;
}

}

#endif
