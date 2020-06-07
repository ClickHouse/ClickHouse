#include <Parsers/parseIntervalKind.h>
#include <Parsers/CommonParsers.h>
#include <Common/IntervalKind.h>


namespace DB
{
bool parseIntervalKind(IParser::Pos & pos, Expected & expected, IParser::Ranges * ranges, IntervalKind & result)
{
    if (ParserKeyword("SECOND").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_SECOND").ignore(pos, expected, ranges)
        || ParserKeyword("SS").ignore(pos, expected, ranges) || ParserKeyword("S").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Second;
        return true;
    }

    if (ParserKeyword("MINUTE").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_MINUTE").ignore(pos, expected, ranges)
        || ParserKeyword("MI").ignore(pos, expected, ranges) || ParserKeyword("N").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Minute;
        return true;
    }

    if (ParserKeyword("HOUR").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_HOUR").ignore(pos, expected, ranges)
        || ParserKeyword("HH").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Hour;
        return true;
    }

    if (ParserKeyword("DAY").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_DAY").ignore(pos, expected, ranges)
        || ParserKeyword("DD").ignore(pos, expected, ranges) || ParserKeyword("D").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Day;
        return true;
    }

    if (ParserKeyword("WEEK").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_WEEK").ignore(pos, expected, ranges)
        || ParserKeyword("WK").ignore(pos, expected, ranges) || ParserKeyword("WW").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Week;
        return true;
    }

    if (ParserKeyword("MONTH").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_MONTH").ignore(pos, expected, ranges)
        || ParserKeyword("MM").ignore(pos, expected, ranges) || ParserKeyword("M").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Month;
        return true;
    }

    if (ParserKeyword("QUARTER").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_QUARTER").ignore(pos, expected, ranges)
        || ParserKeyword("QQ").ignore(pos, expected, ranges) || ParserKeyword("Q").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Quarter;
        return true;
    }

    if (ParserKeyword("YEAR").ignore(pos, expected, ranges) || ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected, ranges)
        || ParserKeyword("YYYY").ignore(pos, expected, ranges) || ParserKeyword("YY").ignore(pos, expected, ranges))
    {
        result = IntervalKind::Year;
        return true;
    }

    return false;
}
}
