#include <Parsers/parseIntervalKind.h>
#include <Parsers/CommonParsers.h>
#include <Common/IntervalKind.h>


namespace DB
{
bool parseIntervalKind(IParser::Pos & pos, Expected & expected, IntervalKind & result)
{
    if (ParserKeyword(Keyword::NANOSECOND).ignore(pos, expected) || ParserKeyword(Keyword::NANOSECONDS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_NANOSECOND).ignore(pos, expected)
        || ParserKeyword(Keyword::NS).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Nanosecond;
        return true;
    }

    if (ParserKeyword(Keyword::MICROSECOND).ignore(pos, expected) || ParserKeyword(Keyword::MICROSECONDS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_MICROSECOND).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Microsecond;
        return true;
    }

    if (ParserKeyword(Keyword::MILLISECOND).ignore(pos, expected) || ParserKeyword(Keyword::MILLISECONDS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_MILLISECOND).ignore(pos, expected)
        || ParserKeyword(Keyword::MS).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Millisecond;
        return true;
    }

    if (ParserKeyword(Keyword::SECOND).ignore(pos, expected) || ParserKeyword(Keyword::SECONDS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_SECOND).ignore(pos, expected)
        || ParserKeyword(Keyword::SS).ignore(pos, expected) || ParserKeyword(Keyword::S).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Second;
        return true;
    }

    if (ParserKeyword(Keyword::MINUTE).ignore(pos, expected) || ParserKeyword(Keyword::MINUTES).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_MINUTE).ignore(pos, expected)
        || ParserKeyword(Keyword::MI).ignore(pos, expected) || ParserKeyword(Keyword::N).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Minute;
        return true;
    }

    if (ParserKeyword(Keyword::HOUR).ignore(pos, expected) || ParserKeyword(Keyword::HOURS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_HOUR).ignore(pos, expected)
        || ParserKeyword(Keyword::HH).ignore(pos, expected) || ParserKeyword(Keyword::H).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Hour;
        return true;
    }

    if (ParserKeyword(Keyword::DAY).ignore(pos, expected) || ParserKeyword(Keyword::DAYS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_DAY).ignore(pos, expected)
        || ParserKeyword(Keyword::DD).ignore(pos, expected) || ParserKeyword(Keyword::D).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Day;
        return true;
    }

    if (ParserKeyword(Keyword::WEEK).ignore(pos, expected) || ParserKeyword(Keyword::WEEKS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_WEEK).ignore(pos, expected)
        || ParserKeyword(Keyword::WK).ignore(pos, expected) || ParserKeyword(Keyword::WW).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Week;
        return true;
    }

    if (ParserKeyword(Keyword::MONTH).ignore(pos, expected) || ParserKeyword(Keyword::MONTHS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_MONTH).ignore(pos, expected)
        || ParserKeyword(Keyword::MM).ignore(pos, expected) || ParserKeyword(Keyword::M).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Month;
        return true;
    }

    if (ParserKeyword(Keyword::QUARTER).ignore(pos, expected) || ParserKeyword(Keyword::QUARTERS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_QUARTER).ignore(pos, expected)
        || ParserKeyword(Keyword::QQ).ignore(pos, expected) || ParserKeyword(Keyword::Q).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Quarter;
        return true;
    }

    if (ParserKeyword(Keyword::YEAR).ignore(pos, expected) || ParserKeyword(Keyword::YEARS).ignore(pos, expected)
        || ParserKeyword(Keyword::SQL_TSI_YEAR).ignore(pos, expected)
        || ParserKeyword(Keyword::YYYY).ignore(pos, expected) || ParserKeyword(Keyword::YY).ignore(pos, expected))
    {
        result = IntervalKind::Kind::Year;
        return true;
    }

    return false;
}
}
