#pragma once

#include "config.h"

#if USE_AVRO

#include <string_view>
#include <vector>

#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>

namespace DB::Iceberg
{

/// Convert a Field to Int64, accepting Int64 and UInt64 (with overflow check).
Int64 fieldToInt64(const Field & value, std::string_view context, std::string_view arg_name);

/// Convert a Field to bool, accepting Bool, UInt64, Int64, and case-insensitive "true"/"false".
bool fieldToBool(const Field & value, std::string_view context, std::string_view arg_name);

/// Parse a Prometheus-style duration string (e.g. "3d", "1d12h30m", "500ms") into milliseconds.
Int64 fieldToPeriodMs(const Field & value, std::string_view context, std::string_view arg_name);

/// Convert a Field containing an Array to vector<Int64>, validating each element.
std::vector<Int64> fieldToInt64Array(const Field & value, std::string_view context, std::string_view arg_name);

/// Iceberg stores lower_bounds and upper_bounds serialized with some custom deserialization as bytes array
/// https://iceberg.apache.org/spec/#appendix-d-single-value-serialization
/// `compensate_rounding` widens a decimal bound, because Iceberg writes it rounded to the integral
/// part; pass false to read the value exactly as the manifest declares it.
std::optional<Field> deserializeFieldFromBinaryRepr(
    const String & str, const DataTypePtr & expected_type, bool lower_bound, bool compensate_rounding = true);

}

#endif
