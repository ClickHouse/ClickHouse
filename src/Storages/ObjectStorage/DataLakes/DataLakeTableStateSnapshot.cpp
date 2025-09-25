
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>


namespace DB {

void serializeDataLakeTableStateSnapshot(DataLakeTableStateSnapshot state, WriteBuffer & out) {
    if (std::holds_alternative<Iceberg::TableStateSnapshot>(state)) {
        writeIntBinary(ICEBERG_TABLE_STATE_SNAPSHOT, out);
       std::get<Iceberg::TableStateSnapshot>(state).serialize(out);
    } else {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization for this DataLakeTableStateSnapshot type is not implemented");
    }
}

DataLakeTableStateSnapshot deserializeDataLakeTableStateSnapshot(ReadBuffer & in)
{
    int type;
    readIntBinary(type, in);
    DataLakeTableStateSnapshot state;
    if (type == ICEBERG_TABLE_STATE_SNAPSHOT)
    {
        Iceberg::TableStateSnapshot iceberg_state = Iceberg::TableStateSnapshot::deserialize(in);
        state = iceberg_state;
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialization for this DataLakeTableStateSnapshot type is not implemented");
    }
    return state;
}
}
