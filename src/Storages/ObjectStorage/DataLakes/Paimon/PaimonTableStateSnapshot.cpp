#include <config.h>

#if USE_AVRO

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Core/ProtocolDefines.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableStateSnapshot.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace Paimon
{

void TableStateSnapshot::serialize(DB::WriteBuffer & out) const
{
    DB::writeIntBinary(snapshot_id, out);
    DB::writeIntBinary(schema_id, out);
    DB::writeStringBinary(base_manifest_list_path, out);
    DB::writeStringBinary(delta_manifest_list_path, out);
    DB::writeStringBinary(commit_kind, out);
    DB::writeIntBinary(commit_time_millis, out);
}

TableStateSnapshot TableStateSnapshot::deserialize(DB::ReadBuffer & in, const int datalake_state_protocol_version)
{
    if (datalake_state_protocol_version <= 0 || datalake_state_protocol_version > DB::DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION)
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Cannot deserialize Paimon::TableStateSnapshot with protocol version {}, maximum supported version is {}",
            datalake_state_protocol_version,
            DB::DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);

    TableStateSnapshot state;
    DB::readIntBinary(state.snapshot_id, in);
    DB::readIntBinary(state.schema_id, in);
    DB::readStringBinary(state.base_manifest_list_path, in);
    DB::readStringBinary(state.delta_manifest_list_path, in);
    DB::readStringBinary(state.commit_kind, in);
    DB::readIntBinary(state.commit_time_millis, in);
    return state;
}

}

#endif
