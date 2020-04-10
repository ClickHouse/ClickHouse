#include <Common/parseGlobs.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <algorithm>
#include <sstream>
#include <cassert>
#include <iomanip>


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
    std::ostringstream oss_for_escaping;
    /// Escaping only characters that not used in glob syntax
    for (const auto & letter : initial_str_with_globs)
    {
        if ((letter == '[') || (letter == ']') || (letter == '|') || (letter == '+') || (letter == '-') || (letter == '(') || (letter == ')'))
            oss_for_escaping << '\\';
        oss_for_escaping << letter;
    }
    std::string escaped_with_globs = oss_for_escaping.str();

    static const re2::RE2 enum_or_range(R"({([\d]+\.\.[\d]+|[^{}*,]+,[^{}*]*[^{}*,])})");    /// regexp for {expr1,expr2,expr3} or {M..N}, where M and N - non-negative integers, expr's should be without {}*,
    re2::StringPiece input(escaped_with_globs);
    re2::StringPiece matched;
    std::ostringstream oss_for_replacing;
    size_t current_index = 0;
    while (RE2::FindAndConsume(&input, enum_or_range, &matched))
    {
        std::string buffer = matched.ToString();
        oss_for_replacing << escaped_with_globs.substr(current_index, matched.data() - escaped_with_globs.data() - current_index - 1) << '(';

        if (buffer.find(',') == std::string::npos)
        {
            size_t range_begin = 0;
            size_t range_end = 0;
            char point;
            std::istringstream iss_range(buffer);
            iss_range >> range_begin >> point >> point >> range_end;
            assert(!iss_range.fail());
            bool leading_zeros = buffer[0] == '0';
            size_t num_len = std::to_string(range_end).size();
            if (leading_zeros)
                oss_for_replacing << std::setfill('0') << std::setw(num_len);
            oss_for_replacing << range_begin;
            for (size_t i = range_begin + 1; i <= range_end; ++i)
            {
                oss_for_replacing << '|';
                if (leading_zeros)
                    oss_for_replacing << std::setfill('0') << std::setw(num_len);
                oss_for_replacing << i;
            }
        }
        else
        {
            std::replace(buffer.begin(), buffer.end(), ',', '|');
            oss_for_replacing << buffer;
        }
        oss_for_replacing << ")";
        current_index = input.data() - escaped_with_globs.data();
    }
    oss_for_replacing << escaped_with_globs.substr(current_index);
    std::string almost_res = oss_for_replacing.str();
    std::ostringstream oss_final_processing;
    for (const auto & letter : almost_res)
    {
        if ((letter == '?') || (letter == '*'))
        {
            oss_final_processing << "[^/]";   /// '?' is any symbol except '/'
            if (letter == '?')
                continue;
        }
        if ((letter == '.') || (letter == '{') || (letter == '}'))
            oss_final_processing << '\\';
        oss_final_processing << letter;
    }
    return oss_final_processing.str();
}
}
