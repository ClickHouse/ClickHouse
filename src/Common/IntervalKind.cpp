#include <Common/IntervalKind.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

const char * IntervalKind::toString() const
{
    switch (kind)
    {
        case IntervalKind::Second: return "Second";
        case IntervalKind::Minute: return "Minute";
        case IntervalKind::Hour: return "Hour";
        case IntervalKind::Day: return "Day";
        case IntervalKind::Week: return "Week";
        case IntervalKind::Month: return "Month";
        case IntervalKind::Quarter: return "Quarter";
        case IntervalKind::Year: return "Year";
    }
    __builtin_unreachable();
}


Int32 IntervalKind::toAvgSeconds() const
{
    switch (kind)
    {
        case IntervalKind::Second: return 1;
        case IntervalKind::Minute: return 60;
        case IntervalKind::Hour: return 3600;
        case IntervalKind::Day: return 86400;
        case IntervalKind::Week: return 604800;
        case IntervalKind::Month: return 2629746;   /// Exactly 1/12 of a year.
        case IntervalKind::Quarter: return 7889238; /// Exactly 1/4 of a year.
        case IntervalKind::Year: return 31556952;   /// The average length of a Gregorian year is equal to 365.2425 days
    }
    __builtin_unreachable();
}


IntervalKind IntervalKind::fromAvgSeconds(Int64 num_seconds)
{
    if (num_seconds)
    {
        if (!(num_seconds % 31556952))
            return IntervalKind::Year;
        if (!(num_seconds % 7889238))
            return IntervalKind::Quarter;
        if (!(num_seconds % 604800))
            return IntervalKind::Week;
        if (!(num_seconds % 2629746))
            return IntervalKind::Month;
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
        case IntervalKind::Second: return "SECOND";
        case IntervalKind::Minute: return "MINUTE";
        case IntervalKind::Hour: return "HOUR";
        case IntervalKind::Day: return "DAY";
        case IntervalKind::Week: return "WEEK";
        case IntervalKind::Month: return "MONTH";
        case IntervalKind::Quarter: return "QUARTER";
        case IntervalKind::Year: return "YEAR";
    }
    __builtin_unreachable();
}


const char * IntervalKind::toDateDiffUnit() const
{
    switch (kind)
    {
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
    __builtin_unreachable();
}


const char * IntervalKind::toNameOfFunctionToIntervalDataType() const
{
    switch (kind)
    {
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
    __builtin_unreachable();
}


const char * IntervalKind::toNameOfFunctionExtractTimePart() const
{
    switch (kind)
    {
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
            throw Exception("The syntax 'EXTRACT(WEEK FROM date)' is not supported, cannot extract the number of a week", ErrorCodes::SYNTAX_ERROR);
        case IntervalKind::Month:
            return "toMonth";
        case IntervalKind::Quarter:
            return "toQuarter";
        case IntervalKind::Year:
            return "toYear";
    }
    __builtin_unreachable();
}
}
