#pragma once

#include <base/types.h>
#include <base/EnumReflection.h>

namespace DB
{
/// Kind of a temporal interval.
struct IntervalKind
{
    enum Kind
    {
        Nanosecond,
        Microsecond,
        Millisecond,
        Second,
        Minute,
        Hour,
        Day,
        Week,
        Month,
        Quarter,
        Year,
    };
    Kind kind = Second;

    IntervalKind(Kind kind_ = Second) : kind(kind_) {} /// NOLINT
    operator Kind() const { return kind; } /// NOLINT

    constexpr std::string_view toString() const { return magic_enum::enum_name(kind); }

    /// Returns number of seconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of seconds.
    Float64 toAvgSeconds() const;

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
    /// Returns false if the conversion unsucceeded.
    /// For example, `IntervalKind::tryParseString('second', result)` returns `result` equals `IntervalKind::Kind::Second`.
    static bool tryParseString(const std::string & kind, IntervalKind::Kind & result);
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
