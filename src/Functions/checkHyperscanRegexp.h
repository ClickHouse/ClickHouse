#pragma once

#include <Common/re2.h>
#include <string_view>
#include <vector>

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
