#include "HTTPAuthenticationScheme.h"

#include <base/types.h>
#include <Poco/String.h>
#include <Common/Exception.h>

#include <magic_enum.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


String toString(HTTPAuthenticationScheme scheme)
{
    return String(magic_enum::enum_name(scheme));
}

HTTPAuthenticationScheme parseHTTPAuthenticationScheme(const String & scheme_str)
{
    auto scheme = magic_enum::enum_cast<HTTPAuthenticationScheme>(Poco::toUpper(scheme_str));
    if (!scheme)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown HTTP authentication scheme: {}. Possible value is 'BASIC'", scheme_str);
    return *scheme;
}

}
