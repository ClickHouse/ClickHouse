#include <Common/IntervalKind.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

Int64 IntervalKind::toAvgNanoseconds() const
{
    static constexpr Int64 NANOSECONDS_PER_MICROSECOND = 1000;
    static constexpr auto NANOSECONDS_PER_MILLISECOND = NANOSECONDS_PER_MICROSECOND * 1000;
    static constexpr auto NANOSECONDS_PER_SECOND = NANOSECONDS_PER_MILLISECOND * 1000;

    switch (kind)
    {
        case IntervalKind::Millisecond:
            return NANOSECONDS_PER_MILLISECOND;
        case IntervalKind::Microsecond:
            return NANOSECONDS_PER_MICROSECOND;
        case IntervalKind::Nanosecond:
            return 1;
        default:
            return toAvgSeconds() * NANOSECONDS_PER_SECOND;
    }

    UNREACHABLE();
}

Int32 IntervalKind::toAvgSeconds() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond:
        case IntervalKind::Microsecond:
        case IntervalKind::Millisecond: return 0;
        case IntervalKind::Second: return 1;
        case IntervalKind::Minute: return 60;
        case IntervalKind::Hour: return 3600;
        case IntervalKind::Day: return 86400;
        case IntervalKind::Week: return 604800;
        case IntervalKind::Month: return 2629746;   /// Exactly 1/12 of a year.
        case IntervalKind::Quarter: return 7889238; /// Exactly 1/4 of a year.
        case IntervalKind::Year: return 31556952;   /// The average length of a Gregorian year is equal to 365.2425 days
    }
    UNREACHABLE();
}

Float64 IntervalKind::toSeconds() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond:
            return 0.000000001;
        case IntervalKind::Microsecond:
            return 0.000001;
        case IntervalKind::Millisecond:
            return 0.001;
        case IntervalKind::Second:
            return 1;
        case IntervalKind::Minute:
            return 60;
        case IntervalKind::Hour:
            return 3600;
        case IntervalKind::Day:
            return 86400;
        case IntervalKind::Week:
            return 604800;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not possible to get precise number of seconds in non-precise interval");
    }
    UNREACHABLE();
}

bool IntervalKind::isFixedLength() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond:
        case IntervalKind::Microsecond:
        case IntervalKind::Millisecond:
        case IntervalKind::Second:
        case IntervalKind::Minute:
        case IntervalKind::Hour:
        case IntervalKind::Day:
        case IntervalKind::Week: return true;
        case IntervalKind::Month:
        case IntervalKind::Quarter:
        case IntervalKind::Year: return false;
    }
    UNREACHABLE();
}

IntervalKind IntervalKind::fromAvgSeconds(Int64 num_seconds)
{
    if (num_seconds)
    {
        if (!(num_seconds % 31556952))
            return IntervalKind::Year;
        if (!(num_seconds % 7889238))
            return IntervalKind::Quarter;
        if (!(num_seconds % 2629746))
            return IntervalKind::Month;
        if (!(num_seconds % 604800))
            return IntervalKind::Week;
        if (!(num_seconds % 86400))
            return IntervalKind::Day;
        if (!(num_seconds % 3600))
            return IntervalKind::Hour;
        if (!(num_seconds % 60))
            return IntervalKind::Minute;
    }
    return IntervalKind::Second;
}


const char * IntervalKind::toKeyword() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond: return "NANOSECOND";
        case IntervalKind::Microsecond: return "MICROSECOND";
        case IntervalKind::Millisecond: return "MILLISECOND";
        case IntervalKind::Second: return "SECOND";
        case IntervalKind::Minute: return "MINUTE";
        case IntervalKind::Hour: return "HOUR";
        case IntervalKind::Day: return "DAY";
        case IntervalKind::Week: return "WEEK";
        case IntervalKind::Month: return "MONTH";
        case IntervalKind::Quarter: return "QUARTER";
        case IntervalKind::Year: return "YEAR";
    }
    UNREACHABLE();
}


const char * IntervalKind::toLowercasedKeyword() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond: return "nanosecond";
        case IntervalKind::Microsecond: return "microsecond";
        case IntervalKind::Millisecond: return "millisecond";
        case IntervalKind::Second: return "second";
        case IntervalKind::Minute: return "minute";
        case IntervalKind::Hour: return "hour";
        case IntervalKind::Day: return "day";
        case IntervalKind::Week: return "week";
        case IntervalKind::Month: return "month";
        case IntervalKind::Quarter: return "quarter";
        case IntervalKind::Year: return "year";
    }
    UNREACHABLE();
}


const char * IntervalKind::toDateDiffUnit() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond:
            return "nanosecond";
        case IntervalKind::Microsecond:
            return "microsecond";
        case IntervalKind::Millisecond:
            return "millisecond";
        case IntervalKind::Second:
            return "second";
        case IntervalKind::Minute:
            return "minute";
        case IntervalKind::Hour:
            return "hour";
        case IntervalKind::Day:
            return "day";
        case IntervalKind::Week:
            return "week";
        case IntervalKind::Month:
            return "month";
        case IntervalKind::Quarter:
            return "quarter";
        case IntervalKind::Year:
            return "year";
    }
    UNREACHABLE();
}


const char * IntervalKind::toNameOfFunctionToIntervalDataType() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond:
            return "toIntervalNanosecond";
        case IntervalKind::Microsecond:
            return "toIntervalMicrosecond";
        case IntervalKind::Millisecond:
            return "toIntervalMillisecond";
        case IntervalKind::Second:
            return "toIntervalSecond";
        case IntervalKind::Minute:
            return "toIntervalMinute";
        case IntervalKind::Hour:
            return "toIntervalHour";
        case IntervalKind::Day:
            return "toIntervalDay";
        case IntervalKind::Week:
            return "toIntervalWeek";
        case IntervalKind::Month:
            return "toIntervalMonth";
        case IntervalKind::Quarter:
            return "toIntervalQuarter";
        case IntervalKind::Year:
            return "toIntervalYear";
    }
    UNREACHABLE();
}


const char * IntervalKind::toNameOfFunctionExtractTimePart() const
{
    switch (kind)
    {
        case IntervalKind::Nanosecond:
            return "toNanosecond";
        case IntervalKind::Microsecond:
            return "toMicrosecond";
        case IntervalKind::Millisecond:
            return "toMillisecond";
        case IntervalKind::Second:
            return "toSecond";
        case IntervalKind::Minute:
            return "toMinute";
        case IntervalKind::Hour:
            return "toHour";
        case IntervalKind::Day:
            return "toDayOfMonth";
        case IntervalKind::Week:
            // TODO: SELECT toRelativeWeekNum(toDate('2017-06-15')) - toRelativeWeekNum(toStartOfYear(toDate('2017-06-15')))
            // else if (ParserKeyword("WEEK").ignore(pos, expected))
            //    function_name = "toRelativeWeekNum";
            throw Exception(ErrorCodes::SYNTAX_ERROR, "The syntax 'EXTRACT(WEEK FROM date)' is not supported, cannot extract the number of a week");
        case IntervalKind::Month:
            return "toMonth";
        case IntervalKind::Quarter:
            return "toQuarter";
        case IntervalKind::Year:
            return "toYear";
    }
    UNREACHABLE();
}


bool IntervalKind::tryParseString(const std::string & kind, IntervalKind::Kind & result)
{
    if ("nanosecond" == kind)
    {
        result = IntervalKind::Nanosecond;
        return true;
    }
    if ("microsecond" == kind)
    {
        result = IntervalKind::Microsecond;
        return true;
    }
    if ("millisecond" == kind)
    {
        result = IntervalKind::Millisecond;
        return true;
    }
    if ("second" == kind)
    {
        result = IntervalKind::Second;
        return true;
    }
    if ("minute" == kind)
    {
        result = IntervalKind::Minute;
        return true;
    }
    if ("hour" == kind)
    {
        result = IntervalKind::Hour;
        return true;
    }
    if ("day" == kind)
    {
        result = IntervalKind::Day;
        return true;
    }
    if ("week" == kind)
    {
        result = IntervalKind::Week;
        return true;
    }
    if ("month" == kind)
    {
        result = IntervalKind::Month;
        return true;
    }
    if ("quarter" == kind)
    {
        result = IntervalKind::Quarter;
        return true;
    }
    if ("year" == kind)
    {
        result = IntervalKind::Year;
        return true;
    }
    return false;
}
}
