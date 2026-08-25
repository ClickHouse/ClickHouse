#include <Parsers/ParserTimeInterval.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIntervalKind.h>

#include <Parsers/ASTTimeInterval.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

ParserTimeInterval::ParserTimeInterval(Options opt) : options(opt) {}
ParserTimeInterval::ParserTimeInterval() = default;

bool ParserTimeInterval::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    CalendarTimeInterval::Intervals intervals;
    while (true)
    {
        ASTPtr value;
        IntervalKind kind;
        if (!ParserNumber{}.parse(pos, value, expected))
            break;
        if (!parseIntervalKind(pos, expected, kind))
            return false;

        UInt64 val;
        if (!value->as<ASTLiteral &>().value.tryGet(val))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Time interval must be an integer");
        intervals.emplace_back(kind, val);
    }

    if (intervals.empty())
        return false;

    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 0; i + 1 < intervals.size(); ++i)
    {
        if (intervals[i].first == intervals[i + 1].first)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Time interval contains multiple {} components", intervals[i].first.toString());
    }

    CalendarTimeInterval interval(intervals);

    if (!options.allow_zero)
        interval.assertPositive();
    if (!options.allow_mixing_calendar_and_clock_units)
        interval.assertSingleUnit();

    auto time_interval = std::make_shared<ASTTimeInterval>();
    time_interval->interval = interval;

    node = time_interval;
    return true;
}

}
