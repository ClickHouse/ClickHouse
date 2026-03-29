#pragma once

#include <Poco/URI.h>

namespace DB::WebIndexPage
{

inline unsigned getEffectivePort(const Poco::URI & uri)
{
    if (const auto port = uri.getPort())
        return port;

    const auto & scheme = uri.getScheme();
    if (scheme == "http")
        return 80;
    if (scheme == "https")
        return 443;

    return 0;
}

inline bool isSameOrigin(const Poco::URI & lhs, const Poco::URI & rhs)
{
    return lhs.getScheme() == rhs.getScheme()
        && lhs.getHost() == rhs.getHost()
        && getEffectivePort(lhs) == getEffectivePort(rhs);
}

}
