#pragma once

#include <base/types.h>
#include <Core/Field.h>

#include <optional>

namespace DB
{

namespace LogsQLUtils
{

/// Parses a LogsQL numeric value: a plain integer or float (possibly with '_' digit separators,
/// hex/octal/binary prefixes, or "inf"/"nan"), a duration ("1h33m", result in nanoseconds),
/// or a byte size ("10KiB", "1MB"). Mirrors tryParseNumber from VictoriaLogs.
std::optional<Float64> tryParseNumber(const String & text);

/// Same as tryParseNumber, but preserves integral values exactly: a value that is a whole
/// number computed from integer components (a plain integer, an integer with a base prefix,
/// or integral duration/byte-size terms) is returned as a `UInt64`/`Int64` field, so that
/// comparisons stay exact across the full 64-bit range instead of rounding through `Float64`.
std::optional<Field> tryParseNumberField(const String & text);

/// Parses a LogsQL duration ("5m", "1h33m55s", "-1.5d") into nanoseconds.
std::optional<Int64> tryParseDuration(const String & text);

/// True if the text looks like the beginning of a number (used to disambiguate `top 5 by (x)` from `top by (x)`).
bool isNumberPrefix(const String & text);

/// Escapes a string for the use inside an RE2 regular expression as a literal.
String escapeRegexp(const String & text);

/// Parses an IPv4 address. Returns the address as a number.
std::optional<UInt32> tryParseIPv4(const String & text);

/// A timestamp parsed from a LogsQL time filter. LogsQL allows to specify timestamps with any precision,
/// e.g. `2023Z` means the whole year 2023. Both bounds are given in nanoseconds since the Unix epoch in UTC
/// when the timezone is known. If the timestamp has no explicit timezone, the bounds are given
/// as civil date-times to be interpreted in the session timezone.
struct TimeValue
{
    /// Nanoseconds since epoch of the start and of the end (exclusive) of the period, when has_timezone = true.
    Int64 start_ns = 0;
    Int64 end_ns = 0;

    /// Civil date-times "YYYY-MM-DD hh:mm:ss[.fraction]" when has_timezone = false.
    String start_civil;
    String end_civil;

    bool has_timezone = false;
};

/// Parses a LogsQL timestamp: 2023Z, 2023-04Z, 2023-04-25Z, 2023-04-25T10Z, 2023-04-25T10:20:30.123Z,
/// with an optional +hh:mm/-hh:mm offset instead of Z, or without any timezone,
/// or a Unix timestamp in seconds/milliseconds/microseconds/nanoseconds.
std::optional<TimeValue> tryParseTimestamp(const String & text);

/// Formats nanoseconds since epoch as a civil UTC date-time string "YYYY-MM-DD hh:mm:ss[.fraction]".
String formatTimestampUTC(Int64 ns);

}

}
