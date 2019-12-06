#pragma once

#include <Core/Types.h>


namespace DB
{
/// Kind of a temporal interval.
struct IntervalKind
{
    enum Kind
    {
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

    IntervalKind(Kind kind_ = Second) : kind(kind_) {}
    operator Kind() const { return kind; }

    const char * toString() const;

    /// Returns number of seconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of seconds.
    Int32 toAvgSeconds() const;

    /// Chooses an interval kind based on number of seconds.
    /// For example, `IntervalKind::fromAvgSeconds(3600)` returns `IntervalKind::Hour`.
    static IntervalKind fromAvgSeconds(Int64 num_seconds);

    /// Returns an uppercased version of what `toString()` returns.
    const char * toKeyword() const;

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
};
}
