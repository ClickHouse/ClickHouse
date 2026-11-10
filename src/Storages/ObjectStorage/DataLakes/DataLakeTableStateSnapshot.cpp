
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/Exception.h>
#include <Core/ProtocolDefines.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeTableStateSnapshot.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace DB
{

void serializeDataLakeTableStateSnapshot(DataLakeTableStateSnapshot state, WriteBuffer & out)
{
    writeVarInt(DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION, out);
    if (std::holds_alternative<Iceberg::TableStateSnapshot>(state))
    {
        writeVarInt(ICEBERG_TABLE_STATE_SNAPSHOT, out);
        std::get<Iceberg::TableStateSnapshot>(state).serialize(out);
    }
#if USE_PARQUET && USE_DELTA_KERNEL_RS
    else if (std::holds_alternative<DeltaLake::TableStateSnapshot>(state))
    {
        writeVarInt(DELTA_LAKE_TABLE_STATE_SNAPSHOT, out);
        std::get<DeltaLake::TableStateSnapshot>(state).serialize(out);
    }
#endif
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization for this DataLakeTableStateSnapshot type is not implemented");
    }
}

DataLakeTableStateSnapshot deserializeDataLakeTableStateSnapshot(ReadBuffer & in)
{
    int protocol_version;
    readVarInt(protocol_version, in);
    if (protocol_version > DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION || protocol_version <= 0)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot deserialize DataLakeTableStateSnapshot with protocol version {}, maximum supported version is {}",
            protocol_version,
            DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);
    if (protocol_version == 1)
    {
        int type;
        readVarInt(type, in);
        if (type == ICEBERG_TABLE_STATE_SNAPSHOT)
        {
            return Iceberg::TableStateSnapshot::deserialize(in, protocol_version);
        }
#if USE_PARQUET && USE_DELTA_KERNEL_RS
        else if (type == DELTA_LAKE_TABLE_STATE_SNAPSHOT)
        {
            return DeltaLake::TableStateSnapshot::deserialize(in, protocol_version);
        }
#endif
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialization for this DataLakeTableStateSnapshot type is not implemented");
        }
    }
    UNREACHABLE();
}
}
