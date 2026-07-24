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
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Lance
{

void TableStateSnapshot::serialize(WriteBuffer & out) const
{
    if (version == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize `Lance::TableStateSnapshot` with zero dataset version");

    writeVarUInt(version, out);
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
    readVarUInt(state.version, in);
    if (state.version == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize `Lance::TableStateSnapshot` with zero dataset version");
    return state;
}

}

#endif
