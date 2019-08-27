#include <Common/parseGlobs.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <algorithm>

namespace DB
{
/* Transforms string from grep-wildcard-syntax ("{N..M}", "{a,b,c}" as in remote table function and "*", "?") to perl-regexp for using re2 library fo matching
 * with such steps:
 * 1) search intervals and enums in {}, replace them by regexp with pipe (expr1|expr2|expr3),
 * 2) search and replace "*" and "?".
 * Before each search need to escape symbols that we would not search.
 */
std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs)
{
    std::string escaped_with_globs;
    escaped_with_globs.reserve(initial_str_with_globs.size());
    /// Escaping only characters that not used in glob syntax
    for (const auto & letter : initial_str_with_globs)
    {
        if ((letter == '[') || (letter == ']') || (letter == '|') || (letter == '+') || (letter == '-') || (letter == '(') || (letter == ')'))
            escaped_with_globs.push_back('\\');
        escaped_with_globs.push_back(letter);
    }

    re2::RE2 enum_or_range(R"({([\d]+\.\.[\d]+|[^{}*,]+,[^{}*]*[^{}*,])})");    /// regexp for {expr1,expr2,expr3} or {M..N}, where M and N - non-negative integers, expr's should be without {}*,
    re2::StringPiece input(escaped_with_globs);
    re2::StringPiece matched(escaped_with_globs);
    size_t current_index = 0;
    std::string almost_regexp;
    almost_regexp.reserve(escaped_with_globs.size());
    while (RE2::FindAndConsume(&input, enum_or_range, &matched))
    {
        std::string buffer = matched.ToString();
        almost_regexp.append(escaped_with_globs.substr(current_index, matched.data() - escaped_with_globs.data() - current_index - 1));

        if (buffer.find(',') == std::string::npos)
        {
            size_t first_point = buffer.find('.');
            std::string first_number = buffer.substr(0, first_point);
            std::string second_number = buffer.substr(first_point + 2, buffer.size() - first_point - 2);
            size_t range_begin = std::stoull(first_number);
            size_t range_end = std::stoull(second_number);
            buffer = std::to_string(range_begin);
            for (size_t i = range_begin + 1; i <= range_end; ++i)
            {
                buffer += "|";
                buffer += std::to_string(i);
            }
        }
        else
        {
            std::replace(buffer.begin(), buffer.end(), ',', '|');
        }
        almost_regexp.append("(" + buffer + ")");
        current_index = input.data() - escaped_with_globs.data();
    }
    almost_regexp += escaped_with_globs.substr(current_index); /////
    std::string result;
    result.reserve(almost_regexp.size());
    for (const auto & letter : almost_regexp)
    {
        if ((letter == '?') || (letter == '*'))
        {
            result += "[^/]";
            if (letter == '?')
                continue;
        }
        if ((letter == '.') || (letter == '{') || (letter == '}') || (letter == '\\'))
            result.push_back('\\');
        result.push_back(letter);
    }
    return result;
}
}
