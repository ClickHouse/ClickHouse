#pragma once

#include <base/types.h>

#include <string_view>

namespace DB::ArrowFlight
{

/// The text of a value of an Arrow type, as the Apache Arrow C++ library formats a scalar (`arrow::Scalar::ToString`).
///
/// The literal of a parameter bound to a prepared statement is made of this text. It is formatted from the value
/// as Arrow stores it, not from a ClickHouse value: the ClickHouse type an Arrow type is read as can have a
/// narrower range (`Date32`, `DateTime`), a time zone the text would depend on (`DateTime64`), or constraints
/// the Arrow type does not have (the scale of a `Decimal`).

/// The unit of the Arrow types with a `TimeUnit`; the values are the ones of the Arrow IPC format.
enum class ArrowTimeUnit : UInt8
{
    Second = 0,
    Millisecond = 1,
    Microsecond = 2,
    Nanosecond = 3,
};

/// `date32`, a number of days since 1970-01-01: `YYYY-MM-DD`.
String formatArrowDate32(Int32 days);

/// `date64`, a number of milliseconds since 1970-01-01: `YYYY-MM-DD`.
String formatArrowDate64(Int64 milliseconds);

/// `timestamp`, a number of units since 1970-01-01 00:00:00 UTC: `YYYY-MM-DD HH:MM:SS[.fraction]` in UTC, followed
/// by `Z` if the type has a time zone. The time zone itself does not change the text.
String formatArrowTimestamp(Int64 value, ArrowTimeUnit unit, bool has_time_zone);

/// `time32` and `time64`, a number of units since midnight: `HH:MM:SS[.fraction]`.
String formatArrowTime(Int64 value, ArrowTimeUnit unit);

/// `decimal32`, `decimal64`, `decimal128` and `decimal256`: `coefficient` is the unscaled value, a little-endian
/// two's complement integer of 4, 8, 16 or 32 bytes. The scale can be negative or greater than the precision:
/// the text is in the scientific notation then, e.g. `1.2E+3`.
String formatArrowDecimal(std::string_view coefficient, Int32 scale);

/// The interval types: `5M`, `1d2ms` and `1M2d3ns`.
String formatArrowMonthInterval(Int32 months);
String formatArrowDayTimeInterval(Int32 days, Int32 milliseconds);
String formatArrowMonthDayNanoInterval(Int32 months, Int32 days, Int64 nanoseconds);

}
