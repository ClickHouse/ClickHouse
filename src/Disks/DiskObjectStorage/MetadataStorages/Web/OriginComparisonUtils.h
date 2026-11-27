#pragma once

#include <base/types.h>

#include <Poco/Exception.h>
#include <Poco/URI.h>

#include <algorithm>
#include <vector>

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

inline bool getNormalizedDecodedPathSegments(const Poco::URI & uri, std::vector<String> & segments)
{
    String decoded_path;
    try
    {
        Poco::URI::decode(uri.getPath(), decoded_path);
    }
    catch (const Poco::Exception &)
    {
        return false;
    }

    String segment;
    for (const auto character : decoded_path)
    {
        if (character != '/')
        {
            segment += character;
            continue;
        }

        if (segment.empty())
            continue;

        if (segment == ".")
        {
            segment.clear();
            continue;
        }

        if (segment == "..")
        {
            if (segments.empty())
                return false;

            segments.pop_back();
            segment.clear();
            continue;
        }

        segments.push_back(segment);
        segment.clear();
    }

    if (!segment.empty())
    {
        if (segment == ".")
            return true;

        if (segment == "..")
        {
            if (segments.empty())
                return false;

            segments.pop_back();
            return true;
        }

        segments.push_back(segment);
    }

    return true;
}

inline bool hasPathPrefix(const Poco::URI & candidate, const Poco::URI & prefix)
{
    if (!candidate.getPath().starts_with(prefix.getPath()))
        return false;

    std::vector<String> candidate_segments;
    std::vector<String> prefix_segments;
    if (!getNormalizedDecodedPathSegments(candidate, candidate_segments)
        || !getNormalizedDecodedPathSegments(prefix, prefix_segments))
        return false;

    return std::mismatch(prefix_segments.begin(), prefix_segments.end(), candidate_segments.begin(), candidate_segments.end()).first
        == prefix_segments.end();
}

/// The effective URL for a listed entry inherits the source URL's query/fragment when the entry
/// itself has none (see `WebObjectStorage::buildURL`). Two entries that differ only by an explicit
/// vs. inherited query/fragment therefore resolve to the same object, so deduplication has to be
/// performed on this normalized form rather than on the raw listing token.
inline String getEffectiveRelativePathForDeduplication(const String & relative, const String & source_url)
{
    Poco::URI relative_uri(relative, false);
    const Poco::URI source_uri(source_url, false);

    if (relative_uri.getRawQuery().empty())
        relative_uri.setRawQuery(source_uri.getRawQuery());
    if (relative_uri.getFragment().empty())
        relative_uri.setFragment(source_uri.getFragment());

    return relative_uri.toString();
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

inline String getPathPrefixRelativeToBase(const Poco::URI & prefix, const Poco::URI & base)
{
    return getRelativePathWithQueryAndFragment(prefix, base);
}

}
