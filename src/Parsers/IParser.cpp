#include <Parsers/IParser.h>
#include <cstring>
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


std::vector<HighlightedRange> expandHighlights(const std::set<HighlightedRange> & highlights)
{
    std::vector<HighlightedRange> result;
    result.reserve(highlights.size());

    for (const auto & range : highlights)
    {
        if (range.highlight != Highlight::string
            && range.highlight != Highlight::string_like
            && range.highlight != Highlight::string_regexp)
        {
            result.push_back(range);
            continue;
        }

        /// Split string/string_like/string_regexp into character-level sub-ranges.
        /// Backslash escape characters are highlighted in all string types.
        /// Metacharacters are highlighted in string_like (%_) and string_regexp (|()^$.[]?*+{:-).
        /// The logic matches Client/ClientBaseHelpers.cpp highlight().
        const char * metacharacters = "";
        if (range.highlight == Highlight::string_like)
            metacharacters = "%_";
        else if (range.highlight == Highlight::string_regexp)
            metacharacters = "|()^$.[]?*+{:-";

        Highlight current_type = Highlight::string;
        const char * current_start = range.begin;
        int escaped = 0;

        for (const char * pos = range.begin; pos < range.end; ++pos)
        {
            Highlight char_type;
            if (*pos == '\\')
            {
                ++escaped;
                char_type = Highlight::string_escape;
            }
            /// The counting of escape characters is quite tricky due to double escaping
            /// of string literals + regexps, and the special logic of interpreting
            /// escape sequences that are not interpreted by the string literals.
            else if ((escaped % 4 == 0 || escaped % 4 == 3) && nullptr != strchr(metacharacters, *pos))
            {
                char_type = Highlight::string_metacharacter;
            }
            else
            {
                char_type = Highlight::string;
                escaped = 0;
            }

            if (char_type != current_type)
            {
                if (pos != current_start)
                    result.push_back({current_start, pos, current_type});
                current_start = pos;
                current_type = char_type;
            }
        }
        if (current_start < range.end)
            result.push_back({current_start, range.end, current_type});
    }

    return result;
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
