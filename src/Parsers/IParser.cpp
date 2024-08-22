#include <Parsers/IParser.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SLOW_PARSING;
}


IParser::Pos & IParser::Pos::operator=(const IParser::Pos & rhs)
{
    depth = rhs.depth;
    max_depth = rhs.max_depth;
    backtracks = std::max(backtracks, rhs.backtracks);
    max_backtracks = rhs.max_backtracks;

    if (rhs < *this)
    {
        ++backtracks;
        if (max_backtracks && backtracks > max_backtracks)
            throw Exception(ErrorCodes::TOO_SLOW_PARSING, "Maximum amount of backtracking ({}) exceeded in the parser. "
                "Consider rising max_parser_backtracks parameter.", max_backtracks);
    }

    TokenIterator::operator=(rhs);

    return *this;
}


template <typename T>
static bool intersects(T a_begin, T a_end, T b_begin, T b_end)
{
    return (a_begin <= b_begin && b_begin < a_end)
        || (b_begin <= a_begin && a_begin < b_end);
}


void Expected::highlight(HighlightedRange range)
{
    if (!enable_highlighting)
        return;

    auto it = highlights.lower_bound(range);
    while (it != highlights.end() && range.begin < it->end)
    {
        if (intersects(range.begin, range.end, it->begin, it->end))
            it = highlights.erase(it);
        else
            ++it;
    }
    highlights.insert(range);
}

}
