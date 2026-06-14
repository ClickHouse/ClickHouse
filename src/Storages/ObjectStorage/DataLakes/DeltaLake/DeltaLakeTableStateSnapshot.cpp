#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeTableStateSnapshot.h>

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DeltaLake
{

void TableStateSnapshot::serialize(DB::WriteBuffer & out) const
{
    DB::writeVarUInt(version, out);
}

TableStateSnapshot TableStateSnapshot::deserialize(DB::ReadBuffer & in, int /*datalake_state_protocol_version*/)
{
    TableStateSnapshot result;
    DB::readVarUInt(result.version, in);
    return result;
}

}

#endif
