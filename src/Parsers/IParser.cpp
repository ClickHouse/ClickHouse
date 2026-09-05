#include <Parsers/IParser.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SLOW_PARSING;
}


/// NOLINTBEGIN(cert-oop54-cpp)
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
/// NOLINTEND(cert-oop54-cpp)


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

    /// Highlights are sorted by their starting position.
    /// lower_bound(range) will find the first highlight where begin >= range.begin.
    /// However, this does not ensure that the previous highlight's end <= range.begin.
    /// By checking the previous highlight, if it exists, we ensure that
    /// for each highlight x and the next one y: x.end <= y.begin, thus preventing any overlap.

    if (it != highlights.begin())
    {
        auto prev_it = std::prev(it);

        if (range.begin < prev_it->end)
            it = prev_it;
    }

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
