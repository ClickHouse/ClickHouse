#pragma once

#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/UDT/CanonicalHash.h>

#include <Core/Types.h>

namespace DB::UDT
{

inline constexpr std::string_view physical_type_fingerprint_domain = "ClickHouse UDT storage fingerprint V2";

/// Hashes the complete canonical DataTypesBinaryEncoding. It must not use
/// encodeDataTypeForHashCalculation(), which intentionally omits Dynamic/JSON
/// parameters that are significant to physical identity.
Digest physicalTypeFingerprint(const DataTypePtr & type);

}
