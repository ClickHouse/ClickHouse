#pragma once

#include <common/types.h>
#include <common/EnumReflection.h>

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

    constexpr std::string_view toString() const { return magic_enum::enum_name(kind); }

    /// Returns number of seconds in one interval.
    /// For `Month`, `Quarter` and `Year` the function returns an average number of seconds.
    Int32 toAvgSeconds() const;

    /// Chooses an interval kind based on number of seconds.
    /// For example, `IntervalKind::fromAvgSeconds(3600)` returns `IntervalKind::Hour`.
    static IntervalKind fromAvgSeconds(Int64 num_seconds);

    /// Returns an uppercase version of toString().
    std::string toKeyword() const
    {
        std::string out { magic_enum::enum_name(kind) };
        std::transform(out.begin(), out.end(), out.begin(), ::toupper);
        return out;
    }

    std::string toLowercasedKeyword() const
    {
        std::string out { magic_enum::enum_name(kind) };
        out[0] = tolower(out[0]);
        return out;
    }

    /**
     * Returns string which can be passed to the `unit` parameter of the dateDiff() function.
     * @example IntervalKind{IntervalKind::Day}.getDateDiffParameter()  == "day"
     */
    std::string toDateDiffUnit() const { return toLowercasedKeyword(); }

    /**
     * Returns name of the function converting a number to the interval data type.
     * @example IntervalKind{IntervalKind::Day}.getToIntervalDataTypeFunctionName() == "toIntervalDay"
     */
    std::string toNameOfFunctionToIntervalDataType() const
    {
        return fmt::format("toInterval{}", magic_enum::enum_name(kind));
    }

    /// Returns the name of the function extracting time part from a date or a time.
    /// For example, `IntervalKind{IntervalKind::Day}.getExtractTimePartFunctionName()`
    /// returns "toDayOfMonth".
    const char * toNameOfFunctionExtractTimePart() const;

    /**
     * Converts lowercase string representation of an interval kind to its IntervalKind equivalent.
     * @example tryParse("second") = {IntervalKind::Second}
     * @example tryParse("Secondd") = std::nullopt
     */
    static std::optional<IntervalKind::Kind> tryParse(std::string_view kind);
};
}
