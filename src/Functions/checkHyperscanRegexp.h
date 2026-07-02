#pragma once

#include <Common/re2.h>
#include <Common/VectorWithMemoryTracking.h>
#include <string_view>
#include <vector>

namespace DB
{

void checkHyperscanRegexp(const VectorWithMemoryTracking<std::string_view> & regexps, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length);

/// Validates the patterns and settings of a multi-pattern hyperscan function (such as `multiMatchAny`).
void checkHyperscanFunctionArguments(
    const VectorWithMemoryTracking<std::string_view> & regexps,
    bool allow_hyperscan,
    size_t max_hyperscan_regexp_length,
    size_t max_hyperscan_regexp_total_length,
    bool reject_expensive_hyperscan_regexps);

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
