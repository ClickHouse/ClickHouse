#include <Common/parseGlobs.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <algorithm>
#include <sstream>

namespace DB
{
/* Transforms string from grep-wildcard-syntax ("{N..M}", "{a,b,c}" as in remote table function and "*", "?") to perl-regexp for using re2 library fo matching
 * with such steps:
 * 1) search intervals like {0..9} and enums like {abc,xyz,qwe} in {}, replace them by regexp with pipe (expr1|expr2|expr3),
 * 2) search and replace "*" and "?".
 * Before each search need to escape symbols that we would not search.
 *
 * There are few examples in unit tests.
 */
std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs)
{
    std::ostringstream oss;
    /// Escaping only characters that not used in glob syntax
    for (const auto & letter : initial_str_with_globs)
    {
        if ((letter == '[') || (letter == ']') || (letter == '|') || (letter == '+') || (letter == '-') || (letter == '(') || (letter == ')'))
            oss << '\\';
        oss << letter;
    }
    std::string escaped_with_globs = oss.str();
    oss.str("");
    static const re2::RE2 enum_or_range(R"({([\d]+\.\.[\d]+|[^{}*,]+,[^{}*]*[^{}*,])})");    /// regexp for {expr1,expr2,expr3} or {M..N}, where M and N - non-negative integers, expr's should be without {}*,
    re2::StringPiece input(escaped_with_globs);
    re2::StringPiece matched;
    size_t current_index = 0;
    while (RE2::FindAndConsume(&input, enum_or_range, &matched))
    {
        std::string buffer = matched.ToString();
        oss << escaped_with_globs.substr(current_index, matched.data() - escaped_with_globs.data() - current_index - 1) << '(';

        if (buffer.find(',') == std::string::npos)
        {
            size_t range_begin, range_end;
            char point;
            std::istringstream iss(buffer);
            iss >> range_begin >> point >> point >> range_end;
            oss << range_begin;
            for (size_t i = range_begin + 1; i <= range_end; ++i)
            {
                oss << '|' << i;
            }
        }
        else
        {
            std::replace(buffer.begin(), buffer.end(), ',', '|');
            oss << buffer;
        }
        oss << ")";
        current_index = input.data() - escaped_with_globs.data();
    }
    oss << escaped_with_globs.substr(current_index);
    std::string almost_res = oss.str();
    oss.str("");
    for (const auto & letter : almost_res)
    {
        if ((letter == '?') || (letter == '*'))
        {
            oss << "[^/]";   /// '?' is any symbol except '/'
            if (letter == '?')
                continue;
        }
        if ((letter == '.') || (letter == '{') || (letter == '}'))
            oss << '\\';
        oss << letter;
    }
    return oss.str();
}
}
