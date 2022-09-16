#include <Parsers/parseIntervalKind.h>
#include <Parsers/CommonParsers.h>
#include <Common/IntervalKind.h>


namespace DB
{
bool parseIntervalKind(IParser::Pos & pos, Expected & expected, IntervalKind & result)
{
    if (ParserKeyword("NANOSECOND").ignore(pos, expected) || ParserKeyword("SQL_TSI_NANOSECOND").ignore(pos, expected)
        || ParserKeyword("NS").ignore(pos, expected))
    {
        result = IntervalKind::Nanosecond;
        return true;
    }

    if (ParserKeyword("MICROSECOND").ignore(pos, expected) || ParserKeyword("SQL_TSI_MICROSECOND").ignore(pos, expected)
        || ParserKeyword("MCS").ignore(pos, expected))
    {
        result = IntervalKind::Microsecond;
        return true;
    }

    if (ParserKeyword("MILLISECOND").ignore(pos, expected) || ParserKeyword("SQL_TSI_MILLISECOND").ignore(pos, expected)
        || ParserKeyword("MS").ignore(pos, expected))
    {
        result = IntervalKind::Millisecond;
        return true;
    }

    if (ParserKeyword("SECOND").ignore(pos, expected) || ParserKeyword("SQL_TSI_SECOND").ignore(pos, expected)
        || ParserKeyword("SS").ignore(pos, expected) || ParserKeyword("S").ignore(pos, expected))
    {
        result = IntervalKind::Second;
        return true;
    }

    if (ParserKeyword("MINUTE").ignore(pos, expected) || ParserKeyword("SQL_TSI_MINUTE").ignore(pos, expected)
        || ParserKeyword("MI").ignore(pos, expected) || ParserKeyword("N").ignore(pos, expected))
    {
        result = IntervalKind::Minute;
        return true;
    }

    if (ParserKeyword("HOUR").ignore(pos, expected) || ParserKeyword("SQL_TSI_HOUR").ignore(pos, expected)
        || ParserKeyword("HH").ignore(pos, expected) || ParserKeyword("H").ignore(pos, expected))
    {
        result = IntervalKind::Hour;
        return true;
    }

    if (ParserKeyword("DAY").ignore(pos, expected) || ParserKeyword("SQL_TSI_DAY").ignore(pos, expected)
        || ParserKeyword("DD").ignore(pos, expected) || ParserKeyword("D").ignore(pos, expected))
    {
        result = IntervalKind::Day;
        return true;
    }

    if (ParserKeyword("WEEK").ignore(pos, expected) || ParserKeyword("SQL_TSI_WEEK").ignore(pos, expected)
        || ParserKeyword("WK").ignore(pos, expected) || ParserKeyword("WW").ignore(pos, expected))
    {
        result = IntervalKind::Week;
        return true;
    }

    if (ParserKeyword("MONTH").ignore(pos, expected) || ParserKeyword("SQL_TSI_MONTH").ignore(pos, expected)
        || ParserKeyword("MM").ignore(pos, expected) || ParserKeyword("M").ignore(pos, expected))
    {
        result = IntervalKind::Month;
        return true;
    }

    if (ParserKeyword("QUARTER").ignore(pos, expected) || ParserKeyword("SQL_TSI_QUARTER").ignore(pos, expected)
        || ParserKeyword("QQ").ignore(pos, expected) || ParserKeyword("Q").ignore(pos, expected))
    {
        result = IntervalKind::Quarter;
        return true;
    }

    if (ParserKeyword("YEAR").ignore(pos, expected) || ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected))
    {
        result = IntervalKind::Year;
        return true;
    }

    return false;
}
}
