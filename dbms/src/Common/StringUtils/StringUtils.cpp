#include "StringUtils.h"
#include <boost/algorithm/string.hpp>

namespace detail
{

bool startsWith(const std::string & s, const char * prefix, size_t prefix_size)
{
    return s.size() >= prefix_size && 0 == memcmp(s.data(), prefix, prefix_size);
}

bool endsWith(const std::string & s, const char * suffix, size_t suffix_size)
{
    return s.size() >= suffix_size && 0 == memcmp(s.data() + s.size() - suffix_size, suffix, suffix_size);
}

}

std::string trim(const std::string & str, const std::function<bool(char)> & predicate)
{
    std::string trimmed = str;
    boost::trim_if(trimmed, predicate);
    return trimmed;
}
