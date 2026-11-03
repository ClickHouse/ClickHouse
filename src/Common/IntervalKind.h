#pragma once

#include <base/types.h>

namespace DB
{
/// Kind of a temporal interval.
struct IntervalKind
{
    /// note: The order and numbers are important and used in binary encoding, append new interval kinds to the end of list.
    enum class Kind : uint8_t
    {
        Nanosecond = 0x00,
        Microsecond = 0x01,
        Millisecond = 0x02,
        Second = 0x03,
        Minute = 0x04,
        Hour = 0x05,
        Day = 0x06,
        Week = 0x07,
        Month = 0x08,
        Quarter = 0x09,
        Year = 0x0A,
    };

    IntervalKind(Kind kind_ = Kind::Second) : kind(kind_) {} /// NOLINT
    operator Kind() const { return kind; } /// NOLINT
    Kind getKind() const { return kind; }

    /// Decodes the interval kind byte of the binary type encoding.
    static IntervalKind fromBinary(UInt8 value);

    UInt8 toBinary() const { return static_cast<UInt8>(kind); }

    std::string_view toString() const;

    /// Returns number of nanoseconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of nanoseconds.
    Int64 toAvgNanoseconds() const;

    /// Returns number of milliseconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of milliseconds.
    Int64 toAvgMilliseconds() const;

    /// Returns number of seconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of seconds.
    Int32 toAvgSeconds() const;

    /// Returns exact number of seconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function raises an error.
    Float64 toSeconds() const;

    /// Chooses an interval kind based on number of seconds.
    /// For example, `IntervalKind::fromAvgSeconds(3600)` returns `IntervalKind::Kind::Hour`.
    static IntervalKind fromAvgSeconds(Int64 num_seconds);

    /// Returns whether IntervalKind has a fixed number of seconds (e.g. Day) or non-fixed (e.g. Month)
    bool isFixedLength() const;

    /// Returns an uppercased version of what `toString` returns.
    const char * toKeyword() const;

    const char * toLowercasedKeyword() const;

    /// Returns the string which can be passed to the `unit` parameter of `dateDiff`. For example, `Day` gives "day".
    const char * toDateDiffUnit() const;

    /// Returns the name of the function converting a number to the interval data type. For example, `Day` gives "toIntervalDay".
    const char * toNameOfFunctionToIntervalDataType() const;

    /// Returns the name of the function extracting time part from a date or a time. For example, `Day` gives "toDayOfMonth".
    const char * toNameOfFunctionExtractTimePart() const;

    /// Inverse of `toNameOfFunctionExtractTimePart`: given a function name like
    /// "toYear", "toMonth", "toDayOfMonth", ... sets `result` to the matching
    /// `IntervalKind` and returns true. Returns false for any other name.
    /// Used to recognise calendar-field extractor functions whose `EXTRACT`-style
    /// dispatch can be redirected onto an `Interval` operand.
    static bool tryParseFromNameOfFunctionExtractTimePart(std::string_view name, IntervalKind & result);

    /// Parses a lowercase interval unit such as "second" into an `IntervalKind`.
    /// Returns false for an unknown name, leaving `result` unchanged.
    static bool tryParseString(std::string_view name, IntervalKind & result);

    auto operator<=>(const IntervalKind & other) const { return kind <=> other.kind; }

private:
    Kind kind = Kind::Second;
};

/// NOLINTNEXTLINE
#define FOR_EACH_INTERVAL_KIND(M) \
    M(Nanosecond) \
    M(Microsecond) \
    M(Millisecond) \
    M(Second) \
    M(Minute) \
    M(Hour) \
    M(Day) \
    M(Week) \
    M(Month) \
    M(Quarter) \
    M(Year)

}
