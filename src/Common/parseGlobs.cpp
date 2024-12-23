#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <algorithm>
#include <sstream>
#include <iomanip>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    std::string_view input(escaped_with_globs);
    std::string_view matched;
    std::ostringstream oss_for_replacing;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss_for_replacing.exceptions(std::ios::failbit);
    size_t current_index = 0;
    while (RE2::FindAndConsume(&input, enum_or_range, &matched))
    {
        std::string buffer(matched);
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
                oss_for_replacing << std::setfill('0') << std::setw(static_cast<int>(output_width));
            oss_for_replacing << range_begin;

            for (size_t i = range_begin + 1; i <= range_end; ++i)
            {
                oss_for_replacing << '|';
                if (leading_zeros)
                    oss_for_replacing << std::setfill('0') << std::setw(static_cast<int>(output_width));
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
    char previous = ' ';
    for (const auto & letter : almost_res)
    {
        if (previous == '*' && letter == '*')
        {
            buf_final_processing << "[^{}]";
        }
        else if ((letter == '?') || (letter == '*'))
        {
            buf_final_processing << "[^/]";   /// '?' is any symbol except '/'
            if (letter == '?')
                continue;
        }
        else if ((letter == '.') || (letter == '{') || (letter == '}'))
            buf_final_processing << '\\';
        buf_final_processing << letter;
        previous = letter;
    }
    return buf_final_processing.str();
}

namespace
{
void expandSelectorGlobImpl(const std::string & path, std::vector<std::string> & for_match_paths_expanded)
{
    /// regexp for {expr1,expr2,....} (a selector glob);
    /// expr1, expr2,... cannot contain any of these: '{', '}', ','
    static const re2::RE2 selector_regex(R"({([^{}*,]+,[^{}*]*[^{}*,])})");

    std::string_view path_view(path);
    std::string_view matched;

    // No (more) selector globs found, quit
    if (!RE2::FindAndConsume(&path_view, selector_regex, &matched))
    {
        for_match_paths_expanded.push_back(path);
        return;
    }

    std::vector<size_t> anchor_positions;
    bool opened = false;
    bool closed = false;

    // Looking for first occurrence of {} selector: write down positions of {, } and all intermediate commas
    for (auto it = path.begin(); it != path.end(); ++it)
    {
        if (*it == '{')
        {
            if (opened)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Unexpected '{{' found in path '{}' at position {}.", path, it - path.begin());
            anchor_positions.push_back(std::distance(path.begin(), it));
            opened = true;
        }
        else if (*it == '}')
        {
            if (!opened)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Unexpected '}}' found in path '{}' at position {}.", path, it - path.begin());
            anchor_positions.push_back(std::distance(path.begin(), it));
            closed = true;
            break;
        }
        else if (*it == ',')
        {
            if (!opened)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Unexpected ',' found in path '{}' at position {}.", path, std::distance(path.begin(), it));
            anchor_positions.push_back(std::distance(path.begin(), it));
        }
    }
    if (!opened || !closed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Invalid {{}} glob in path {}.", path);

    // generate result: prefix/{a,b,c}/suffix -> [prefix/a/suffix, prefix/b/suffix, prefix/c/suffix]
    std::string common_prefix = path.substr(0, anchor_positions.front());
    std::string common_suffix = path.substr(anchor_positions.back() + 1);
    for (size_t i = 1; i < anchor_positions.size(); ++i)
    {
        std::string current_selection =
                path.substr(anchor_positions[i-1] + 1, (anchor_positions[i] - anchor_positions[i-1] - 1));

        std::string expanded_matcher = common_prefix + current_selection + common_suffix;
        expandSelectorGlobImpl(expanded_matcher, for_match_paths_expanded);
    }
}
}

std::vector<std::string> expandSelectionGlob(const std::string & path)
{
    std::vector<std::string> result;
    expandSelectorGlobImpl(path, result);
    return result;
}
}
