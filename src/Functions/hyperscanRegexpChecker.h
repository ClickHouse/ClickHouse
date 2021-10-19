#pragma once

#include <base/StringRef.h>

namespace DB
{

void checkRegexp(const std::vector<StringRef> & refs, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length);

}
