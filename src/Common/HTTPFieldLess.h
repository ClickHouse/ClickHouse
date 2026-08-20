#pragma once

#include <string_view>

#include <Common/StringUtils.h>

namespace DB
{

/// Comparator for HTTP field (header) names.
/// Per RFC 9110 §5.1, field names are case-insensitive tokens composed of ASCII characters.
/// Only letters are folded via isAlphaASCII + toLowerIfAlphaASCII; punctuation and other
/// valid token characters are left intact, so `Foo^Bar` and `Foo~Bar` remain distinct.
struct HTTPFieldLess
{
    bool operator()(std::string_view a, std::string_view b) const
    {
        return std::lexicographical_compare(
            a.begin(), a.end(), b.begin(), b.end(),
            [](char x, char y)
            {
                return (isAlphaASCII(x) ? toLowerIfAlphaASCII(x) : x)
                     < (isAlphaASCII(y) ? toLowerIfAlphaASCII(y) : y);
            });
    }
};

}
