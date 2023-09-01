#pragma once

#include <string_view>
#include <vector>

namespace DB
{

void checkHyperscanRegexp(const std::vector<std::string_view> & regexps, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length);

}
