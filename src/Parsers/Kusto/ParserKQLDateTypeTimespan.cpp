#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <cstdlib>
#include <unordered_map>
#include <format>

namespace DB
{

bool ParserKQLDateTypeTimespan :: parseImpl(Pos & pos,  [[maybe_unused]] ASTPtr & node, Expected & expected)
{
    const String token(pos->begin,pos->end);
    const char * current_word = pos->begin;
    expected.add(pos, current_word);

    if (!parseConstKQLTimespan(token))
        return false;

     return true;
}

double ParserKQLDateTypeTimespan :: toSeconds()
{
    switch (time_span_unit) 
    {
        case KQLTimespanUint::day:
            return time_span * 24 * 60 * 60;
        case KQLTimespanUint::hour:
            return time_span * 60 * 60;
        case KQLTimespanUint::minute:
            return time_span *  60;
        case KQLTimespanUint::second:
            return time_span ;
        case KQLTimespanUint::millisec:
            return time_span / 1000.0;
        case KQLTimespanUint::microsec:
            return time_span / 1000000.0;
        case KQLTimespanUint::nanosec:
            return time_span / 1000000000.0;
        case KQLTimespanUint::tick:
            return time_span / 10000000000.0;
    }
}

bool ParserKQLDateTypeTimespan :: parseConstKQLTimespan(const String & text)
{
    std::unordered_map <String, KQLTimespanUint> TimespanSuffixes =
    {
        {"d", KQLTimespanUint::day},
        {"day", KQLTimespanUint::day},
        {"days", KQLTimespanUint::day},
        {"h", KQLTimespanUint::hour},
        {"hr", KQLTimespanUint::hour},
        {"hrs", KQLTimespanUint::hour},
        {"hour", KQLTimespanUint::hour},
        {"hours", KQLTimespanUint::hour},
        {"m", KQLTimespanUint::minute},
        {"min", KQLTimespanUint::minute},
        {"minute", KQLTimespanUint::minute},
        {"minutes", KQLTimespanUint::minute},
        {"s", KQLTimespanUint::second},
        {"sec", KQLTimespanUint::second},
        {"second", KQLTimespanUint::second},
        {"seconds", KQLTimespanUint::second},
        {"ms", KQLTimespanUint::millisec},
        {"milli", KQLTimespanUint::millisec},
        {"millis", KQLTimespanUint::millisec},
        {"millisec", KQLTimespanUint::millisec},
        {"millisecond", KQLTimespanUint::millisec},
        {"milliseconds", KQLTimespanUint::millisec},
        {"micro", KQLTimespanUint::microsec},
        {"micros", KQLTimespanUint::microsec},
        {"microsec", KQLTimespanUint::microsec},
        {"microsecond", KQLTimespanUint::microsec},
        {"microseconds", KQLTimespanUint::microsec},
        {"nano", KQLTimespanUint::nanosec},
        {"nanos", KQLTimespanUint::nanosec},
        {"nanosec", KQLTimespanUint::nanosec},
        {"nanosecond", KQLTimespanUint::nanosec},
        {"nanoseconds", KQLTimespanUint::nanosec},
        {"tick", KQLTimespanUint::tick},
        {"ticks", KQLTimespanUint::tick}
    };


    const char * ptr = text.c_str();

    auto scanDigit = [&](const char *start)
    {
        auto index = start;
        while (isdigit(*index))
            ++index;
        return index > start ? index - start : -1;
    };

    int number_len = scanDigit(ptr);
    if (number_len <= 0)
        return false;

    if (*(ptr + number_len) == '.')
    {
        auto fractionLen = scanDigit(ptr + number_len + 1);
        if (fractionLen >= 0)
        {
            number_len += fractionLen + 1;
        }
    }

    String timespan_suffix(ptr + number_len, ptr+text.size());
    if (TimespanSuffixes.find(timespan_suffix) == TimespanSuffixes.end())
        return false;

    time_span = std::stod(String(ptr, ptr + number_len));
    time_span_unit =TimespanSuffixes[timespan_suffix] ;

    return true;
}

}
