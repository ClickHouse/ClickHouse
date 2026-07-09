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
    Kind kind = Kind::Second;

    IntervalKind(Kind kind_ = Kind::Second) : kind(kind_) {} /// NOLINT
    operator Kind() const { return kind; } /// NOLINT

    std::string_view toString() const;

    /// Returns number of nanoseconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of nanoseconds.
    Int64 toAvgNanoseconds() const;

    /// Returns number of seconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of seconds.
    Int32 toAvgSeconds() const;

    /// Returns exact number of seconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function raises an error.
    Float64 toSeconds() const;

    /// Chooses an interval kind based on number of seconds.
    /// For example, `IntervalKind::fromAvgSeconds(3600)` returns `IntervalKind::Hour`.
    static IntervalKind fromAvgSeconds(Int64 num_seconds);

    /// Returns whether IntervalKind has a fixed number of seconds (e.g. Day) or non-fixed(e.g. Month)
    bool isFixedLength() const;

    /// Returns an uppercased version of what `toString()` returns.
    const char * toKeyword() const;

    const char * toLowercasedKeyword() const;

    /// Returns the string which can be passed to the `unit` parameter of the dateDiff() function.
    /// For example, `IntervalKind{IntervalKind::Day}.getDateDiffParameter()` returns "day".
    const char * toDateDiffUnit() const;

    /// Returns the name of the function converting a number to the interval data type.
    /// For example, `IntervalKind{IntervalKind::Day}.getToIntervalDataTypeFunctionName()`
    /// returns "toIntervalDay".
    const char * toNameOfFunctionToIntervalDataType() const;

    /// Returns the name of the function extracting time part from a date or a time.
    /// For example, `IntervalKind{IntervalKind::Day}.getExtractTimePartFunctionName()`
    /// returns "toDayOfMonth".
    const char * toNameOfFunctionExtractTimePart() const;

    /// Converts the string representation of an interval kind to its IntervalKind equivalent.
    /// Returns false if the conversion did not succeed.
    /// For example, `IntervalKind::tryParseString('second', result)` returns `result` equals `IntervalKind::Kind::Second`.
    static bool tryParseString(const std::string & kind, IntervalKind::Kind & result);

    auto operator<=>(const IntervalKind & other) const { return kind <=> other.kind; }
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
