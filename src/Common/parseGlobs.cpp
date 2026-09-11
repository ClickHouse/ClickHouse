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
            char point = 0;
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
    for (size_t i = 0; i < almost_res.size();)
    {
        /// `**/` matches zero or more directory components, but only when `**` forms a whole
        /// path segment: it must be bounded by `/` (or the start of the string) on the left and
        /// by `/` on the right. This matches conventional glob semantics (e.g. Bash `globstar`,
        /// where `**` is special only as a complete path component) and keeps this helper
        /// consistent with the segment-by-segment local listing in `StorageFile`, which gives
        /// zero-level semantics only to a path segment that is exactly `**`. A `**` adjacent to
        /// other characters in a segment (e.g. `a**`, `?**`, or a run of 3+ stars like `***/`)
        /// is not a globstar and keeps the legacy character-by-character expansion below.
        /// Use `[^/]` so directory names containing `{` or `}` are still matched. We look at
        /// `almost_res[i - 1]` directly rather than tracking the previous character, because the
        /// `?` branch below uses `continue` and does not update `previous` — checking the source
        /// string is robust against that.
        if (i + 2 < almost_res.size()
            && almost_res[i] == '*'
            && almost_res[i + 1] == '*'
            && almost_res[i + 2] == '/'
            && (i == 0 || almost_res[i - 1] == '/'))
        {
            buf_final_processing << "([^/]*/)*";
            i += 3;
            previous = '/';
            continue;
        }

        /// For every other case (including `**` not followed by `/`, and runs of 3+ stars),
        /// keep the original character-by-character logic so the legacy regex is preserved.
        const char letter = almost_res[i];
        if (previous == '*' && letter == '*')
        {
            buf_final_processing << "[^{}]";
        }
        else if ((letter == '?') || (letter == '*'))
        {
            buf_final_processing << "[^/]"; /// '?' is any symbol except '/'
            if (letter == '?')
            {
                ++i;
                continue;
            }
        }
        else if ((letter == '.') || (letter == '{') || (letter == '}'))
            buf_final_processing << '\\';
        buf_final_processing << letter;
        previous = letter;
        ++i;
    }
    return buf_final_processing.str();
}

namespace
{

/// Bounds on the `{a,b,c}` selector glob expansion below.
///
/// The expansion is a Cartesian product: `{a,b}{c,d}{e,f}...` produces as many paths as the product
/// of the group sizes, so a pattern of a few hundred bytes is enough to ask for more paths than
/// could ever be listed. The expansion happens while a table function is being resolved, where the
/// query is not cancellable and is not stopped by `max_memory_usage`, so it has to bound itself.
constexpr size_t MAX_SELECTOR_GLOBS = 1000;
constexpr size_t MAX_EXPANDED_PATHS = 100000;
constexpr size_t MAX_EXPANDED_BYTES = 64 * 1024 * 1024;

/// Whether `path` has no `{a,b,c}` selector glob left to enumerate. A `{N..M}` range glob anywhere
/// in the path also stops the enumeration: ranges are turned into a regexp by
/// `makeRegexpPatternFromGlobs` instead of being expanded into separate paths.
bool noSelectorGlobsToExpand(std::string_view path)
{
    /// enum_regexp does not match elements of one char, e.g. {a}.tsv
    bool definitely_no_selector_globs = path.find_first_of("{}") == std::string_view::npos;
    if (!definitely_no_selector_globs)
    {
        auto left_bracket_pos = path.find_first_of('{');
        auto right_bracket_pos = path.find_first_of('}');

        auto is_this_enum_of_one_char =
            left_bracket_pos != std::string_view::npos
            && right_bracket_pos != std::string_view::npos
            && (right_bracket_pos - left_bracket_pos) == 2;

        definitely_no_selector_globs = !is_this_enum_of_one_char;
    }

    if (!definitely_no_selector_globs)
        return false;

    /// range_glob regex is stricter than enum_glob, so we need to check
    /// if whatever matched enum_glob is also range_glob. If it does match it too -- this is a range glob.
    bool is_this_enum_glob = RE2::PartialMatch(path, Regexps::instance().enum_regex);
    bool is_this_range_glob = RE2::PartialMatch(path, Regexps::instance().range_regex);
    return !is_this_enum_glob || is_this_range_glob;
}

/// One `{a,b,c}` selector glob of a path, together with the literal text preceding it.
/// Both are views into the path.
struct SelectorGlob
{
    std::string_view literal_before;
    std::vector<std::string_view> alternatives;
};

}

std::vector<std::string> expandSelectionGlob(const std::string & path)
{
    /// Split the path into its `{a,b,c}` selector globs and the literal text in between, looking at
    /// one glob at a time, from left to right. What is a glob does not depend on the alternatives
    /// picked for the globs before it, so the path is split once and not once per expanded path.
    std::vector<SelectorGlob> globs;
    std::string_view tail(path);

    /// The number of paths the globs seen so far expand to, a running Cartesian product.
    size_t num_paths = 1;

    while (!noSelectorGlobsToExpand(tail))
    {
        if (globs.size() >= MAX_SELECTOR_GLOBS)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The path has more than {} '{{}}' globs to expand.", MAX_SELECTOR_GLOBS);

        /// The offset of `tail` in `path`, to report positions in the path the user has written.
        const size_t tail_offset = path.size() - tail.size();

        /// Looking for the first occurrence of a {} selector: write down the positions of {, } and
        /// all intermediate commas.
        std::vector<size_t> anchor_positions;
        bool opened = false;
        bool closed = false;

        for (size_t i = 0; i < tail.size(); ++i)
        {
            if (tail[i] == '{')
            {
                if (opened)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Unexpected '{{' found in path '{}' at position {}.", path, tail_offset + i);
                anchor_positions.push_back(i);
                opened = true;
            }
            else if (tail[i] == '}')
            {
                if (!opened)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Unexpected '}}' found in path '{}' at position {}.", path, tail_offset + i);
                anchor_positions.push_back(i);
                closed = true;
                break;
            }
            else if (tail[i] == ',')
            {
                if (!opened)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Unexpected ',' found in path '{}' at position {}.", path, tail_offset + i);
                anchor_positions.push_back(i);

                /// Refuse an oversized group while it is being scanned, and not after it has been
                /// materialized: otherwise a single selector with an enormous number of
                /// alternatives - a whole file passed as a path by `file(file(...))` - still costs
                /// memory proportional to its number of commas before the limit below is reached.
                /// After `k` commas the group has at least `k + 1` alternatives, and
                /// `anchor_positions` holds the '{' and those `k` commas.
                if (num_paths > MAX_EXPANDED_PATHS / anchor_positions.size())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "The '{{}}' globs in the path expand to more than {} paths.", MAX_EXPANDED_PATHS);
            }
        }
        if (!opened || !closed)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid {{}} glob in path {}.", path);

        SelectorGlob glob;
        glob.literal_before = tail.substr(0, anchor_positions.front());
        for (size_t i = 1; i < anchor_positions.size(); ++i)
            glob.alternatives.push_back(
                tail.substr(anchor_positions[i - 1] + 1, anchor_positions[i] - anchor_positions[i - 1] - 1));

        /// Refuse a combinatorial explosion before generating anything.
        if (num_paths > MAX_EXPANDED_PATHS / glob.alternatives.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The '{{}}' globs in the path expand to more than {} paths.", MAX_EXPANDED_PATHS);
        num_paths *= glob.alternatives.size();

        globs.push_back(std::move(glob));
        tail = tail.substr(anchor_positions.back() + 1);
    }

    /// generate result: prefix/{a,b,c}/suffix -> [prefix/a/suffix, prefix/b/suffix, prefix/c/suffix]
    std::vector<std::string> result;
    result.reserve(num_paths);

    std::vector<size_t> alternative_indices(globs.size(), 0);
    size_t expanded_bytes = 0;

    for (size_t path_index = 0; path_index < num_paths; ++path_index)
    {
        std::string expanded;
        expanded.reserve(path.size());  /// An expanded path is never longer than the pattern.
        for (size_t i = 0; i < globs.size(); ++i)
            expanded.append(globs[i].literal_before).append(globs[i].alternatives[alternative_indices[i]]);
        expanded.append(tail);

        expanded_bytes += expanded.size();
        if (expanded_bytes > MAX_EXPANDED_BYTES)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The '{{}}' globs in the path expand to more than {} bytes of paths.", MAX_EXPANDED_BYTES);

        result.push_back(std::move(expanded));

        /// The last glob changes fastest, so that the paths are generated in the order of the pattern.
        for (size_t i = globs.size(); i-- > 0;)
        {
            if (++alternative_indices[i] < globs[i].alternatives.size())
                break;
            alternative_indices[i] = 0;
        }
    }

    return result;
}
}
