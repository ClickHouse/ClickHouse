#pragma once

#include <base/Decimal.h>


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

}
