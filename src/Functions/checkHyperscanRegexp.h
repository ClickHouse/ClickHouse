#pragma once

#include <string_view>
#include <vector>

#ifdef __clang__
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif
#include <re2/re2.h>
#ifdef __clang__
#  pragma clang diagnostic pop
#endif

namespace DB
{

void checkHyperscanRegexp(const std::vector<std::string_view> & regexps, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length);

/// Regexp evaluation with hyperscan can be slow for certain patterns due to NFA state explosion. Try to identify such patterns on a
/// best-effort basis.

class SlowWithHyperscanChecker
{
public:
    SlowWithHyperscanChecker();
    bool isSlow(std::string_view regexp);

private:
    bool isSlowOneRepeat(std::string_view regexp);
    bool isSlowTwoRepeats(std::string_view regexp);
    re2::RE2 searcher_one_repeat;
    re2::RE2 searcher_two_repeats;
};

}
