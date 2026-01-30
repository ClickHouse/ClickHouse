#include <Common/parseGlobs.h>
#include <Common/re2.h>

#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/readIntText.h>
#include <Interpreters/Context_fwd.h>
#include <base/arithmeticOverflow.h>
#include <base/defines.h>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <optional>
#include <sstream>
#include <iomanip>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
struct Regexps
{
    static const Regexps & instance()
    {
        static Regexps regexps;
        return regexps;
    }

    /// regexp for {M..N}, where M and N - non-negative integers
    re2::RE2 range_regex{R"({([\d]+\.\.[\d]+)})"};

    /// regexp for {expr1,expr2,expr3}, expr's should be without "{", "}", "*" and ","
    re2::RE2 enum_regex{R"({([^{}*,]+[^{}*]*[^{}*,])})"};
};
}

bool containsRangeGlob(const std::string & input)
{
    return RE2::PartialMatch(input, Regexps::instance().range_regex);
}

bool containsOnlyEnumGlobs(const std::string & input)
{
    return input.find_first_of("*?") == String::npos && !containsRangeGlob(input);
}

bool hasExactlyOneBracketsExpansion(const std::string & input)
{
    return std::count(input.begin(), input.end(), '{') == 1 && containsOnlyEnumGlobs(input);
}

namespace BetterGlob
{

std::string Expression::dump() const
{
    switch (type())
    {
        case ExpressionType::CONSTANT:
            return std::string(std::get<std::string_view>(getData()));
        case ExpressionType::WILDCARD:
            return dumpWildcard();
        case ExpressionType::RANGE:
            return dumpRange();
        case ExpressionType::ENUM:
            return dumpEnum();
    }

    UNREACHABLE();
}

std::string Expression::asRegex() const
{
    switch (type())
    {
        case ExpressionType::CONSTANT:
            return escape(std::get<std::string_view>(getData()));
        case ExpressionType::WILDCARD:
            return wildcardAsRegex();
        case ExpressionType::RANGE:
            return rangeAsRegex();
        case ExpressionType::ENUM:
            return enumAsRegex();
    }

    UNREACHABLE();
}

std::string Expression::dumpWildcard() const
{
    const auto & wildcard = std::get<WildcardType>(getData());

    switch (wildcard)
    {
        case WildcardType::QUESTION:
            return "?";
        case WildcardType::SINGLE_ASTERISK:
            return "*";
        case WildcardType::DOUBLE_ASTERISK:
            return "**";
    }

    UNREACHABLE();
}

std::string Expression::escape(const std::string_view input) const
{
    WriteBufferFromOwnString buf_for_escaping;

    for (const auto & letter : input)
    {
        if ((letter == '[') || (letter == ']') || (letter == '|') || (letter == '+') || (letter == '-') || (letter == '(') || (letter == ')') || (letter == '\\'))
            buf_for_escaping << '\\';
        if ((letter == '.') || (letter == '{') || (letter == '}'))
            buf_for_escaping << '\\';
        buf_for_escaping << letter;
    }

    return buf_for_escaping.str();

}

std::string Expression::wildcardAsRegex() const
{
    const auto & wildcard = std::get<WildcardType>(getData());

    switch (wildcard)
    {
        case WildcardType::QUESTION:
            return "[^/]";
        case WildcardType::SINGLE_ASTERISK:
            return "[^/]*";
        case WildcardType::DOUBLE_ASTERISK:
            return "[^{}]*";
    }

    UNREACHABLE();
}

std::string Expression::enumAsRegex() const
{
    const auto separator = '|';

    std::string result = "(";
    const auto & enum_values = std::get<std::vector<std::string_view>>(getData());

    for (const auto & e: enum_values)
    {
         result += escape(e);
         result += separator;
    }

    result.back() = ')';
    return result;
}

std::string Expression::rangeAsRegex() const
{
    std::string result = "(";

    const auto & range = std::get<Range>(getData());

    const size_t value = std::min(range.start, range.end);
    const size_t width = ((range.start_zero_padded && range.start_digit_count > 1) || (range.end_zero_padded && range.end_digit_count > 1))
        ? std::max(range.start_digit_count, range.end_digit_count)
        : 0;

    for (size_t i = 0; i <= cardinality(); ++i)
    {
        result += fmt::format(
            "{:0>{}}",
            value + i,
            width
        );
        result += '|';
    }

    result.back() = ')';

    return result;
}

std::string Expression::dumpEnum(char separator) const
{
    std::string result = "{";
    const auto & enum_values = std::get<std::vector<std::string_view>>(getData());

    for (const auto & e: enum_values)
    {
         result += e;
         result += separator;
    }

    result.back() = '}';
    return result;
}

std::string Expression::dumpRange() const
{
    std::string result = "{";
    const auto & range = std::get<Range>(getData());

    result += fmt::format("{:0>{}}", range.start, range.start_digit_count);
    result += "..";
    result += fmt::format("{:0>{}}", range.end, range.end_digit_count);
    result += "}";

    return result;
}

size_t Expression::cardinality() const
{
    switch (type())
    {
        case ExpressionType::CONSTANT:
            return 1;
        case ExpressionType::WILDCARD:
            return std::numeric_limits<size_t>::max();
        case ExpressionType::ENUM:
            return std::get<std::vector<std::string_view>>(getData()).size();
        case ExpressionType::RANGE:
        {
            Range range = std::get<Range>(getData());
            const size_t range_len = (range.start > range.end)
                ? range.start - range.end
                : range.end - range.start;
            return range_len;
        }
    }

    UNREACHABLE();
}

size_t GlobString::cardinality() const
{
    size_t result = 1;

    for (const auto & expression : expressions)
    {
        size_t expression_cardinality = expression.cardinality();
        if (expression_cardinality == std::numeric_limits<size_t>::max())
            return std::numeric_limits<size_t>::max();

        bool overflow = common::mulOverflow(result, expression_cardinality, result);
        if (overflow)
            return std::numeric_limits<size_t>::max();
    }

    return result;
}

bool GlobString::hasExactlyOneEnum() const
{
    size_t enum_counter = 0;

    for (const auto & expression : expressions)
    {
        switch (expression.type())
        {
            case ExpressionType::CONSTANT:
                continue;
            case ExpressionType::WILDCARD:
                return false;
            case ExpressionType::RANGE:
                return false;
            case ExpressionType::ENUM:
                enum_counter++;
                break;
        }
    }

    return enum_counter == 1;
};

std::string_view GlobString::consumeConstantExpression(const std::string_view & input) const
{
    auto first_nonconstant = input.find_first_of("{*?");

    if (first_nonconstant == std::string::npos)
        return input;

    return input.substr(0, first_nonconstant);
}

std::string_view GlobString::consumeMatcher(const std::string_view & input) const
{
    auto first_curly_closing_brace = input.find_first_of('}');

    if (first_curly_closing_brace == std::string::npos)
        return {};

    return input.substr(0, first_curly_closing_brace + 1);
}

std::vector<std::string_view> GlobString::tryParseEnumMatcher(const std::string_view & input) const
{
    assert(input.length() > 2);
    assert(input.front() == '{');
    assert(input.back() == '}');

    auto separator = ',';

    std::vector<std::string_view> enum_elements;
    std::string_view contents = input.substr(1, input.length() - 2);

    size_t search_start_pos = 0;
    while (true)
    {
        auto next_separator_pos = contents.find(separator, search_start_pos);

        if (next_separator_pos == std::string_view::npos)
        {
            enum_elements.emplace_back(contents.begin() + search_start_pos, contents.end());
            break;
        }

        enum_elements.emplace_back(contents.begin() + search_start_pos, contents.begin() + next_separator_pos);
        search_start_pos = next_separator_pos + 1;
    }

    return enum_elements;
}

std::optional<Range> GlobString::tryParseRangeMatcher(const std::string_view & input) const
{
    assert(input.length() > 2);

    /// Range matcher must contain "..", like in "{0..10}".
    auto double_dot_pos = input.find_first_of("..");
    if (double_dot_pos == std::string_view::npos)
        return std::nullopt;

    Range range;
    ReadBufferFromString read_buffer(input);
    size_t first_digit_pos;

    bool ok = true;

    ok &= checkChar('{', read_buffer);

    if (!ok)
        return std::nullopt;

    first_digit_pos = read_buffer.offset();

    ok &= tryReadIntText(range.start, read_buffer);
    if (!ok)
        return std::nullopt;

    range.start_digit_count = read_buffer.offset() - first_digit_pos;
    range.start_zero_padded = input[first_digit_pos] == '0';

    ok &= checkChar('.', read_buffer);
    ok &= checkChar('.', read_buffer);

    if (!ok)
        return std::nullopt;

    first_digit_pos = read_buffer.offset();

    ok &= tryReadIntText(range.end, read_buffer);
    if (!ok)
        return std::nullopt;

    range.end_digit_count = read_buffer.offset() - first_digit_pos;
    range.end_zero_padded = input[first_digit_pos] == '0';

    ok &= checkChar('}', read_buffer);

    if (!ok)
        return std::nullopt;

    return range;
}

GlobString::GlobString(std::string input): input_data(std::move(input))
{
    parse();
}

std::string GlobString::dump() const
{
    std::string result;

    for (const auto & e: getExpressions())
        result += e.dump();

    return result;
}

std::string GlobString::asRegex() const
{
    std::string result;

    for (const auto & e: getExpressions())
        result += e.asRegex();

    return result;
}

void GlobString::parse()
{
    if (input_data.empty())
        return;

    std::string_view input = input_data;

    size_t position = 0;
    while (position < input.length())
    {
        if (input[position] == '?' || input[position] == '*')
        {
            has_globs = true;
            has_question_or_asterisk = true;

            if (position + 1 < input.length() && input[position] == input[position + 1] && input[position] == '*')
            {
                expressions.emplace_back(WildcardType::DOUBLE_ASTERISK);
                position += 2;

                continue;
            }

            /// FIXME move to WildcardType enum
            switch (input[position])
            {
                case '?':
                    expressions.emplace_back(WildcardType::QUESTION);
                    break;
                case '*':
                    expressions.emplace_back(WildcardType::SINGLE_ASTERISK);
                    break;
                default:
                    UNREACHABLE();
            }
            position += 1;

            continue;
        }
        else if (input[position] == '{')  /// NOLINT
        {
            /// FIXME why do we even need to support double braces?
            if (position + 1 > input.length() && input[position + 1] == '{')
            {
                expressions.emplace_back("{");
                position += 1;
            }

            auto matcher_expression = consumeMatcher(input.substr(position));

            auto range = tryParseRangeMatcher(matcher_expression);
            if (range.has_value())
            {
                position += matcher_expression.length();
                expressions.push_back(Expression(range.value()));

                has_globs = true;
                has_ranges = true;

                continue;
            }

            auto enum_matcher = tryParseEnumMatcher(matcher_expression);

            if (enum_matcher.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected an enum expression, but read 0 bytes.");  // FIXME

            position += matcher_expression.length();
            expressions.push_back(Expression(enum_matcher));

            has_globs = true;
            has_enums = true;

            continue;
        }
        else
        {
            auto constant_expression = consumeConstantExpression(input.substr(position));

            if (constant_expression.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a constant expression, but read 0 bytes.");  // FIXME

            position += constant_expression.length();
            expressions.push_back(Expression(constant_expression));

            continue;
        }
    }
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

    std::string_view matched;
    std::string_view input(escaped_with_globs);
    std::ostringstream oss_for_replacing; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss_for_replacing.exceptions(std::ios::failbit);
    size_t current_index = 0;

    /// We may find range and enum globs in any order, let's look for both types on each iteration.
    while (true)
    {
        std::string_view matched_range;
        std::string_view matched_enum;

        auto did_match_range = RE2::PartialMatch(input, Regexps::instance().range_regex, &matched_range);
        auto did_match_enum = RE2::PartialMatch(input, Regexps::instance().enum_regex, &matched_enum);

        /// Enum regex matches ranges, so if they both match and point to the same data,
        /// it is a range.
        if (did_match_range && did_match_enum && matched_range.data() == matched_enum.data())
            did_match_enum = false;

        /// We matched a range, and range comes earlier than enum
        if (did_match_range && (!did_match_enum || matched_range.data() < matched_enum.data()))
        {
            RE2::FindAndConsume(&input, Regexps::instance().range_regex, &matched);
            std::string buffer(matched);
            oss_for_replacing << escaped_with_globs.substr(current_index, matched_range.data() - escaped_with_globs.data() - current_index - 1) << '(';

            size_t range_begin = 0;
            size_t range_end = 0;
            char point;
            ReadBufferFromString buf_range(buffer);
            buf_range >> range_begin >> point >> point >> range_end;

            size_t range_begin_width = buffer.find('.');
            size_t range_end_width = buffer.size() - buffer.find_last_of('.') - 1;
            bool leading_zeros = buffer[0] == '0';
            size_t output_width = 0;

            if (range_begin > range_end) /// Descending Sequence {20..15} {9..01}
            {
                std::swap(range_begin,range_end);
                leading_zeros = buffer[buffer.find_last_of('.') + 1] == '0';
                std::swap(range_begin_width,range_end_width);
            }
            if (range_begin_width == 1 && leading_zeros)
                output_width = 1; /// Special Case: {0..10} {0..999}
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

            oss_for_replacing << ")";
            current_index = input.data() - escaped_with_globs.data();
        }
        /// We matched enum, and it comes earlier than range.
        else if (did_match_enum && (!did_match_range || matched_enum.data() < matched_range.data()))
        {
            RE2::FindAndConsume(&input, Regexps::instance().enum_regex, &matched);
            std::string buffer(matched);

            oss_for_replacing << escaped_with_globs.substr(current_index, matched.data() - escaped_with_globs.data() - current_index - 1) << '(';
            std::replace(buffer.begin(), buffer.end(), ',', '|');

            oss_for_replacing << buffer;
            oss_for_replacing << ")";

            current_index = input.data() - escaped_with_globs.data();
        }
        else
            break;
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
            buf_final_processing << "[^/]"; /// '?' is any symbol except '/'
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
    std::string_view path_view(path);
    std::string_view matched;

    /// enum_regexp does not match elements of one char, e.g. {a}.tsv
    auto definitely_no_selector_globs = path.find_first_of("{}") == std::string::npos;
    if (!definitely_no_selector_globs)
    {
        auto left_bracket_pos = path.find_first_of('{');
        auto right_bracket_pos = path.find_first_of('}');

        auto is_this_enum_of_one_char =
            left_bracket_pos != std::string::npos
            && right_bracket_pos != std::string::npos
            && (right_bracket_pos - left_bracket_pos) == 2;

        definitely_no_selector_globs = !is_this_enum_of_one_char;
    }

    auto is_this_range_glob = RE2::PartialMatch(path_view, Regexps::instance().range_regex, &matched);
    auto is_this_enum_glob = RE2::PartialMatch(path_view, Regexps::instance().enum_regex, &matched);

    /// No (more) selector globs found, quit
    ///
    /// range_glob regex is stricter than enum_glob, so we need to check
    /// if whatever matched enum_glob is also range_glob. If it does match it too -- this is a range glob.
    if ((!is_this_enum_glob || (is_this_range_glob && is_this_enum_glob)) && definitely_no_selector_globs)
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

    /// generate result: prefix/{a,b,c}/suffix -> [prefix/a/suffix, prefix/b/suffix, prefix/c/suffix]
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
