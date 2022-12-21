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

namespace
{

std::string rangeToPattern(size_t begin, size_t end, size_t width)
{
    std::string pattern;
    std::string x = fmt::format("{0:0{1}}", begin, width);
    std::string y = fmt::format("{0:0{1}}", end, width);

    size_t any_digits_count = 0;
    size_t leading_zeros = 0;
    size_t i = 0;

    for (; i < x.size() && i < y.size(); ++i)
    {
        if (x[i] == '0' && y[i] == '0')
            ++leading_zeros;
        else
            break;
    }

    if (leading_zeros > 2)
        pattern += fmt::format("0{{{}}}", leading_zeros);
    else
        i = 0;

    for (; i < x.size() && i < y.size(); ++i)
    {
        if (x[i] == y[i])
            pattern += x[i];
        else if (x[i] != '0' || y[i] != '9')
            pattern += fmt::format("[{}-{}]", x[i], y[i]);
        else
            ++any_digits_count;
    }

    if (any_digits_count > 0)
        pattern += "\\d";

    if (any_digits_count > 1)
        pattern += fmt::format("{{{}}}", any_digits_count);

    return pattern;
}

size_t fillByNines(size_t i, int nines_count)
{
    return i - (i % intExp10(nines_count)) + intExp10(nines_count) - 1;
}

size_t fillByzeros(size_t i, int zeros_count)
{
    return i - (i % intExp10(zeros_count));
}

std::vector<size_t> splitToRanges(size_t begin, size_t end)
{
    std::vector<size_t> stops = {end};

    int nines_count = 1;
    size_t stop = fillByNines(begin, nines_count);
    while (begin <= stop && stop < end && nines_count < 20)
    {
        stops.push_back(stop);
        ++nines_count;
        stop = fillByNines(begin, nines_count);
    }

    int zeros_count = 1;
    stop = fillByzeros(end + 1, zeros_count);
    stop = stop ? stop - 1 : 0;
    while (begin < stop && stop <= end && zeros_count < 20)
    {
        stops.push_back(stop);
        ++zeros_count;
        stop = fillByzeros(end + 1, zeros_count);
        stop = stop ? stop - 1 : 0;
    }

    std::sort(stops.begin(), stops.end());
    stops.erase(std::unique(stops.begin(), stops.end()), stops.end());
    return stops;
}

std::vector<std::string> splitToPatterns(size_t begin, size_t end, size_t width)
{
    std::vector<std::string> subpatterns;

    size_t start = begin;
    std::vector<size_t> ranges = splitToRanges(begin, end);
    for (auto stop : ranges)
    {
        subpatterns.push_back(rangeToPattern(start, stop, width));
        start = stop + 1;
    }

    return subpatterns;
}

std::string regexForRange(size_t begin, size_t end, size_t width)
{
    std::string result;
    std::vector<std::string> patterns = splitToPatterns(begin, end, width);
    for (size_t i = 0; i < patterns.size(); i++)
    {
        if (i != 0)
            result += '|';

        result += patterns[i];
    }

    return result;
}

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
    re2::StringPiece input(escaped_with_globs);
    re2::StringPiece matched;
    std::ostringstream oss_for_replacing;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss_for_replacing.exceptions(std::ios::failbit);
    size_t current_index = 0;
    while (RE2::FindAndConsume(&input, enum_or_range, &matched))
    {
        std::string buffer = matched.ToString();
        oss_for_replacing << escaped_with_globs.substr(current_index, matched.data() - escaped_with_globs.data() - current_index - 1) << '(';

        /// Check whether it is a numeric range
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
            else if (leading_zeros)
                output_width = std::max(range_begin_width, range_end_width);

            oss_for_replacing << regexForRange(range_begin, range_end, output_width);
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
}
