#include <Common/IntervalKind.h>
#include <Common/Exception.h>

#include <base/EnumReflection.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

std::string_view IntervalKind::toString() const
{
    return magic_enum::enum_name(kind);
}

Int64 IntervalKind::toAvgNanoseconds() const
{
    static constexpr Int64 NANOSECONDS_PER_MICROSECOND = 1000;
    static constexpr auto NANOSECONDS_PER_MILLISECOND = NANOSECONDS_PER_MICROSECOND * 1000;
    static constexpr auto NANOSECONDS_PER_SECOND = NANOSECONDS_PER_MILLISECOND * 1000;

    switch (kind)
    {
        case IntervalKind::Kind::Millisecond:
            return NANOSECONDS_PER_MILLISECOND;
        case IntervalKind::Kind::Microsecond:
            return NANOSECONDS_PER_MICROSECOND;
        case IntervalKind::Kind::Nanosecond:
            return 1;
        default:
            return toAvgSeconds() * NANOSECONDS_PER_SECOND;
    }
}

Int32 IntervalKind::toAvgSeconds() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond:
        case IntervalKind::Kind::Microsecond:
        case IntervalKind::Kind::Millisecond: return 0;
        case IntervalKind::Kind::Second: return 1;
        case IntervalKind::Kind::Minute: return 60;
        case IntervalKind::Kind::Hour: return 3600;
        case IntervalKind::Kind::Day: return 86400;
        case IntervalKind::Kind::Week: return 604800;
        case IntervalKind::Kind::Month: return 2629746;   /// Exactly 1/12 of a year.
        case IntervalKind::Kind::Quarter: return 7889238; /// Exactly 1/4 of a year.
        case IntervalKind::Kind::Year: return 31556952;   /// The average length of a Gregorian year is equal to 365.2425 days
    }
}

Float64 IntervalKind::toSeconds() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond:
            return 0.000000001;
        case IntervalKind::Kind::Microsecond:
            return 0.000001;
        case IntervalKind::Kind::Millisecond:
            return 0.001;
        case IntervalKind::Kind::Second:
            return 1;
        case IntervalKind::Kind::Minute:
            return 60;
        case IntervalKind::Kind::Hour:
            return 3600;
        case IntervalKind::Kind::Day:
            return 86400;
        case IntervalKind::Kind::Week:
            return 604800;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not possible to get precise number of seconds in non-precise interval");
    }
}

bool IntervalKind::isFixedLength() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond:
        case IntervalKind::Kind::Microsecond:
        case IntervalKind::Kind::Millisecond:
        case IntervalKind::Kind::Second:
        case IntervalKind::Kind::Minute:
        case IntervalKind::Kind::Hour:
        case IntervalKind::Kind::Day:
        case IntervalKind::Kind::Week: return true;
        case IntervalKind::Kind::Month:
        case IntervalKind::Kind::Quarter:
        case IntervalKind::Kind::Year: return false;
    }
}

IntervalKind IntervalKind::fromAvgSeconds(Int64 num_seconds)
{
    if (num_seconds)
    {
        if (!(num_seconds % 31556952))
            return IntervalKind::Kind::Year;
        if (!(num_seconds % 7889238))
            return IntervalKind::Kind::Quarter;
        if (!(num_seconds % 2629746))
            return IntervalKind::Kind::Month;
        if (!(num_seconds % 604800))
            return IntervalKind::Kind::Week;
        if (!(num_seconds % 86400))
            return IntervalKind::Kind::Day;
        if (!(num_seconds % 3600))
            return IntervalKind::Kind::Hour;
        if (!(num_seconds % 60))
            return IntervalKind::Kind::Minute;
    }
    return IntervalKind::Kind::Second;
}


const char * IntervalKind::toKeyword() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond: return "NANOSECOND";
        case IntervalKind::Kind::Microsecond: return "MICROSECOND";
        case IntervalKind::Kind::Millisecond: return "MILLISECOND";
        case IntervalKind::Kind::Second: return "SECOND";
        case IntervalKind::Kind::Minute: return "MINUTE";
        case IntervalKind::Kind::Hour: return "HOUR";
        case IntervalKind::Kind::Day: return "DAY";
        case IntervalKind::Kind::Week: return "WEEK";
        case IntervalKind::Kind::Month: return "MONTH";
        case IntervalKind::Kind::Quarter: return "QUARTER";
        case IntervalKind::Kind::Year: return "YEAR";
    }
}


const char * IntervalKind::toLowercasedKeyword() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond: return "nanosecond";
        case IntervalKind::Kind::Microsecond: return "microsecond";
        case IntervalKind::Kind::Millisecond: return "millisecond";
        case IntervalKind::Kind::Second: return "second";
        case IntervalKind::Kind::Minute: return "minute";
        case IntervalKind::Kind::Hour: return "hour";
        case IntervalKind::Kind::Day: return "day";
        case IntervalKind::Kind::Week: return "week";
        case IntervalKind::Kind::Month: return "month";
        case IntervalKind::Kind::Quarter: return "quarter";
        case IntervalKind::Kind::Year: return "year";
    }
}


const char * IntervalKind::toDateDiffUnit() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond:
            return "nanosecond";
        case IntervalKind::Kind::Microsecond:
            return "microsecond";
        case IntervalKind::Kind::Millisecond:
            return "millisecond";
        case IntervalKind::Kind::Second:
            return "second";
        case IntervalKind::Kind::Minute:
            return "minute";
        case IntervalKind::Kind::Hour:
            return "hour";
        case IntervalKind::Kind::Day:
            return "day";
        case IntervalKind::Kind::Week:
            return "week";
        case IntervalKind::Kind::Month:
            return "month";
        case IntervalKind::Kind::Quarter:
            return "quarter";
        case IntervalKind::Kind::Year:
            return "year";
    }
}


const char * IntervalKind::toNameOfFunctionToIntervalDataType() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond:
            return "toIntervalNanosecond";
        case IntervalKind::Kind::Microsecond:
            return "toIntervalMicrosecond";
        case IntervalKind::Kind::Millisecond:
            return "toIntervalMillisecond";
        case IntervalKind::Kind::Second:
            return "toIntervalSecond";
        case IntervalKind::Kind::Minute:
            return "toIntervalMinute";
        case IntervalKind::Kind::Hour:
            return "toIntervalHour";
        case IntervalKind::Kind::Day:
            return "toIntervalDay";
        case IntervalKind::Kind::Week:
            return "toIntervalWeek";
        case IntervalKind::Kind::Month:
            return "toIntervalMonth";
        case IntervalKind::Kind::Quarter:
            return "toIntervalQuarter";
        case IntervalKind::Kind::Year:
            return "toIntervalYear";
    }
}


const char * IntervalKind::toNameOfFunctionExtractTimePart() const
{
    switch (kind)
    {
        case IntervalKind::Kind::Nanosecond:
            return "toNanosecond";
        case IntervalKind::Kind::Microsecond:
            return "toMicrosecond";
        case IntervalKind::Kind::Millisecond:
            return "toMillisecond";
        case IntervalKind::Kind::Second:
            return "toSecond";
        case IntervalKind::Kind::Minute:
            return "toMinute";
        case IntervalKind::Kind::Hour:
            return "toHour";
        case IntervalKind::Kind::Day:
            return "toDayOfMonth";
        case IntervalKind::Kind::Week:
            // TODO: SELECT toRelativeWeekNum(toDate('2017-06-15')) - toRelativeWeekNum(toStartOfYear(toDate('2017-06-15')))
            // else if (ParserKeyword(Keyword::WEEK).ignore(pos, expected))
            //    function_name = "toRelativeWeekNum";
            throw Exception(ErrorCodes::SYNTAX_ERROR, "The syntax 'EXTRACT(WEEK FROM date)' is not supported, cannot extract the number of a week");
        case IntervalKind::Kind::Month:
            return "toMonth";
        case IntervalKind::Kind::Quarter:
            return "toQuarter";
        case IntervalKind::Kind::Year:
            return "toYear";
    }
}


bool IntervalKind::tryParseString(const std::string & kind, IntervalKind::Kind & result)
{
    if ("nanosecond" == kind)
    {
        result = IntervalKind::Kind::Nanosecond;
        return true;
    }
    if ("microsecond" == kind)
    {
        result = IntervalKind::Kind::Microsecond;
        return true;
    }
    if ("millisecond" == kind)
    {
        result = IntervalKind::Kind::Millisecond;
        return true;
    }
    if ("second" == kind)
    {
        result = IntervalKind::Kind::Second;
        return true;
    }
    if ("minute" == kind)
    {
        result = IntervalKind::Kind::Minute;
        return true;
    }
    if ("hour" == kind)
    {
        result = IntervalKind::Kind::Hour;
        return true;
    }
    if ("day" == kind)
    {
        result = IntervalKind::Kind::Day;
        return true;
    }
    if ("week" == kind)
    {
        result = IntervalKind::Kind::Week;
        return true;
    }
    if ("month" == kind)
    {
        result = IntervalKind::Kind::Month;
        return true;
    }
    if ("quarter" == kind)
    {
        result = IntervalKind::Kind::Quarter;
        return true;
    }
    if ("year" == kind)
    {
        result = IntervalKind::Kind::Year;
        return true;
    }
    return false;
}
}
