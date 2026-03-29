#pragma once

#include <base/types.h>

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

inline bool hasPathPrefix(const Poco::URI & candidate, const Poco::URI & prefix)
{
    return candidate.getPath().starts_with(prefix.getPath());
}

inline String getRelativePathWithQueryAndFragment(const Poco::URI & candidate, const Poco::URI & base)
{
    if (!hasPathPrefix(candidate, base))
        return {};

    String relative = candidate.getPath().substr(base.getPath().size());

    if (!candidate.getRawQuery().empty())
        relative += "?" + candidate.getRawQuery();
    if (!candidate.getFragment().empty())
        relative += "#" + candidate.getFragment();

    return relative;
}

}
