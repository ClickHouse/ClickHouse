#include <Common/parseGlobs.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <algorithm>
#include <sstream>
#include <iomanip>


namespace DB
{
/* Transforms string from grep-wildcard-syntax ("{N..M}", "{a,b,c}" as in remote table function and "*", "?") to perl-regexp for using re2 library for matching
 * with such steps:
 * 1) search intervals like {0..9} and enums like {abc,xyz,qwe} in {}, replace them by regexp with pipe (expr1|expr2|expr3),
 * 2) search and replace "*" and "?".
 * Before each search need to escape symbols that we would not search.
 *
 * There are few examples in unit tests.
 */
std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs)
{
    /// FIXME make it better
    WriteBufferFromOwnString buf_for_escaping;
    /// Escaping only characters that not used in glob syntax
    for (const auto & letter : initial_str_with_globs)
    {
        if ((letter == '[') || (letter == ']') || (letter == '|') || (letter == '+') || (letter == '-') || (letter == '(') || (letter == ')') || (letter == '\\'))
            buf_for_escaping << '\\';
        buf_for_escaping << letter;
    }
    std::string escaped_with_globs = buf_for_escaping.str();

    static const re2::RE2 enum_or_range(R"({([\d]+\.\.[\d]+|[^{}*,]+,[^{}*]*[^{}*,])})");    /// regexp for {expr1,expr2,expr3} or {M..N}, where M and N - non-negative integers, expr's should be without "{", "}", "*" and ","
    re2::StringPiece input(escaped_with_globs);
    re2::StringPiece matched;
    std::ostringstream oss_for_replacing;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss_for_replacing.exceptions(std::ios::failbit);
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
            ReadBufferFromString buf_range(buffer);
            buf_range >> range_begin >> point >> point >> range_end;

            size_t range_begin_width = buffer.find('.');
            size_t range_end_width = buffer.size() - buffer.find_last_of('.') - 1;
            bool leading_zeros = buffer[0] == '0';
            size_t output_width = 0;

            if (range_begin > range_end)    //Descending Sequence {20..15} {9..01}
            {
                std::swap(range_begin,range_end);
                leading_zeros = buffer[buffer.find_last_of('.')+1]=='0';
                std::swap(range_begin_width,range_end_width);
            }
            if (range_begin_width == 1 && leading_zeros)
                output_width = 1;   ///Special Case: {0..10} {0..999}
            else
                output_width = std::max(range_begin_width, range_end_width);

            if (leading_zeros)
                oss_for_replacing << std::setfill('0') << std::setw(output_width);
            oss_for_replacing << range_begin;

            for (size_t i = range_begin + 1; i <= range_end; ++i)
            {
                oss_for_replacing << '|';
                if (leading_zeros)
                    oss_for_replacing << std::setfill('0') << std::setw(output_width);
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
    WriteBufferFromOwnString buf_final_processing;
    for (const auto & letter : almost_res)
    {
        if ((letter == '?') || (letter == '*'))
        {
            buf_final_processing << "[^/]";   /// '?' is any symbol except '/'
            if (letter == '?')
                continue;
        }
        if ((letter == '.') || (letter == '{') || (letter == '}'))
            buf_final_processing << '\\';
        buf_final_processing << letter;
    }
    return buf_final_processing.str();
}
}
