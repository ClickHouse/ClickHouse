#include <Functions/checkHyperscanRegexp.h>

#include <Common/Exception.h>

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
                throw Exception("Regexp length too large", ErrorCodes::BAD_ARGUMENTS);
            total_regexp_length += regexp.size();
        }

        if (max_hyperscan_regexp_total_length > 0 && total_regexp_length > max_hyperscan_regexp_total_length)
            throw Exception("Total regexp lengths too large", ErrorCodes::BAD_ARGUMENTS);
    }
}

}
