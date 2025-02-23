#include <Functions/checkHyperscanRegexp.h>

#include <Common/Exception.h>
#include <charconv>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void checkHyperscanRegexp(const std::vector<std::string_view> & regexps, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length)
{
    if (max_hyperscan_regexp_length > 0 || max_hyperscan_regexp_total_length > 0)
    {
        size_t total_regexp_length = 0;
        for (const auto & regexp : regexps)
        {
            if (max_hyperscan_regexp_length > 0 && regexp.size() > max_hyperscan_regexp_length)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Regexp length too large ({} > {})", regexp.size(), max_hyperscan_regexp_length);
            total_regexp_length += regexp.size();
        }

        if (max_hyperscan_regexp_total_length > 0 && total_regexp_length > max_hyperscan_regexp_total_length)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Total regexp lengths too large ({} > {})",
                            total_regexp_length, max_hyperscan_regexp_total_length);
    }
}

namespace
{

bool isLargerThanFifty(std::string_view str)
{
    int number;
    auto [_, ec] = std::from_chars(str.begin(), str.end(), number);
    if (ec != std::errc())
        return false;
    return number > 50;
}

}

/// Check for sub-patterns of the form x{n} or x{n,} can be expensive. Ignore spaces before/after n and m.
bool SlowWithHyperscanChecker::isSlowOneRepeat(std::string_view regexp)
{
    std::string_view haystack(regexp.data(), regexp.size());
    std::string_view matches[2];
    size_t start_pos = 0;
    while (start_pos < haystack.size())
    {
        if (searcher_one_repeat.Match(haystack, start_pos, haystack.size(), re2::RE2::Anchor::UNANCHORED, matches, 2))
        {
            const auto & match = matches[0];
            start_pos = (matches[0].data() - haystack.data()) + match.size(); // new start pos = prefix before match + match length
            const auto & submatch = matches[1];
            if (isLargerThanFifty({submatch.data(), submatch.size()}))
                return true;
        }
        else
            break;
    }
    return false;
}

/// Check if sub-patterns of the form x{n,m} can be expensive. Ignore spaces before/after n and m.
bool SlowWithHyperscanChecker::isSlowTwoRepeats(std::string_view regexp)
{
    std::string_view haystack(regexp.data(), regexp.size());
    std::string_view matches[3];
    size_t start_pos = 0;
    while (start_pos < haystack.size())
    {
        if (searcher_two_repeats.Match(haystack, start_pos, haystack.size(), re2::RE2::Anchor::UNANCHORED, matches, 3))
        {
            const auto & match = matches[0];
            start_pos = (matches[0].data() - haystack.data()) + match.size(); // new start pos = prefix before match + match length
            const auto & submatch1 = matches[1];
            const auto & submatch2 = matches[2];
            if (isLargerThanFifty({submatch1.data(), submatch1.size()})
             || isLargerThanFifty({submatch2.data(), submatch2.size()}))
                return true;
        }
        else
            break;
    }
    return false;
}

SlowWithHyperscanChecker::SlowWithHyperscanChecker()
    : searcher_one_repeat(R"(\{\s*([\d]+)\s*,?\s*})")
    , searcher_two_repeats(R"(\{\s*([\d]+)\s*,\s*([\d]+)\s*\})")
{}

bool SlowWithHyperscanChecker::isSlow(std::string_view regexp)
{
    if (isSlowOneRepeat(regexp))
        return true;
    if (isSlowTwoRepeats(regexp))
        return true;
    return false;
}

}
