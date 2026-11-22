#pragma once

#include <base/Decimal.h>

#include <optional>
#include <string_view>


namespace DB
{
class ConstantValue;
class Field;

/// Parses a timestamp from a string.
DateTime64 parseTimeSeriesTimestamp(const String & str, UInt32 timestamp_scale);

/// Extracts a timestamp from a Field.
DateTime64 parseTimeSeriesTimestamp(const Field & field, UInt32 timestamp_scale);

/// Parses a duration from a string.
Decimal64 parseTimeSeriesDuration(const String & str, UInt32 duration_scale);

/// Extracts a duration from a Field.
Decimal64 parseTimeSeriesDuration(const Field & field, UInt32 duration_scale);

/// Extracts a timestamp or a duration from a constant (e.g. an evaluated table function argument), by its exact
/// data type. `Bool` and NULL are rejected.
DateTime64 parseTimeSeriesTimestamp(const ConstantValue & value, UInt32 timestamp_scale);
Decimal64 parseTimeSeriesDuration(const ConstantValue & value, UInt32 duration_scale);

/// Converts a setting in seconds to a duration, rounding up; a value below 1 microsecond gives nullopt.
std::optional<Decimal64> convertSecondsSettingToTimeSeriesDuration(std::string_view setting_name, Float64 seconds, UInt32 duration_scale);

}
