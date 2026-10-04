#include "config.h"

#if USE_LANCE

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>

#include <algorithm>

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

namespace
{
bool isZeroDigest(const TableStateSnapshot::Digest & digest)
{
    return std::all_of(digest.begin(), digest.end(), [](UInt8 byte) { return byte == 0; });
}
}

void TableStateSnapshot::validate(int error_code) const
{
    if (version == 0)
        throw Exception(error_code, "`Lance::TableStateSnapshot` has zero dataset version");
    if (manifest_size == 0)
        throw Exception(error_code, "`Lance::TableStateSnapshot` has zero manifest size");
    if (isZeroDigest(manifest_id))
        throw Exception(error_code, "`Lance::TableStateSnapshot` has an empty manifest ID");
    if (isZeroDigest(manifest_sha256))
        throw Exception(error_code, "`Lance::TableStateSnapshot` has an empty manifest SHA-256");
    if (!has_etag && !isZeroDigest(etag_sha256))
        throw Exception(error_code, "`Lance::TableStateSnapshot` has an `e_tag` digest without an `e_tag`");
    if (has_etag && isZeroDigest(etag_sha256))
        throw Exception(error_code, "`Lance::TableStateSnapshot` has an empty `e_tag` digest");
}

void TableStateSnapshot::serialize(WriteBuffer & out) const
{
    validate(ErrorCodes::LOGICAL_ERROR);

    writeVarUInt(version, out);
    out.write(reinterpret_cast<const char *>(manifest_id.data()), manifest_id.size());
    writeVarUInt(manifest_size, out);
    out.write(reinterpret_cast<const char *>(manifest_sha256.data()), manifest_sha256.size());
    writeBinary(static_cast<UInt8>(has_etag), out);
    out.write(reinterpret_cast<const char *>(etag_sha256.data()), etag_sha256.size());
}

TableStateSnapshot TableStateSnapshot::deserialize(ReadBuffer & in, int datalake_state_protocol_version)
{
    if (datalake_state_protocol_version <= 0 || datalake_state_protocol_version > DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot deserialize Lance::TableStateSnapshot with protocol version {}, maximum supported version is {}",
            datalake_state_protocol_version,
            DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);

    if (datalake_state_protocol_version == 1)
    {
        UInt64 legacy_version = 0;
        readVarUInt(legacy_version, in);
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Version-only `Lance::TableStateSnapshot` protocol for version {} is unsafe and cannot be used for reading",
            legacy_version);
    }

    TableStateSnapshot state;
    readVarUInt(state.version, in);
    in.readStrict(reinterpret_cast<char *>(state.manifest_id.data()), state.manifest_id.size());
    readVarUInt(state.manifest_size, in);
    in.readStrict(reinterpret_cast<char *>(state.manifest_sha256.data()), state.manifest_sha256.size());
    UInt8 has_etag = 0;
    readBinary(has_etag, in);
    if (has_etag > 1)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid `Lance::TableStateSnapshot` `has_etag` value {}",
            static_cast<unsigned>(has_etag));
    state.has_etag = has_etag != 0;
    in.readStrict(reinterpret_cast<char *>(state.etag_sha256.data()), state.etag_sha256.size());
    state.validate(ErrorCodes::INCORRECT_DATA);
    return state;
}

}

#endif
