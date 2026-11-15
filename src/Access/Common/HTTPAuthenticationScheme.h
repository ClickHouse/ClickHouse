#pragma once

#include <base/types.h>

namespace DB
{

enum class HTTPAuthenticationScheme
{
    BASIC,
    BEARER
};


String toString(HTTPAuthenticationScheme scheme);
HTTPAuthenticationScheme parseHTTPAuthenticationScheme(const String & scheme_str);
}
