#include <Disks/DiskObjectStorage/ObjectStorages/ParallelListingGlobPredicate.h>

#include <Common/parseGlobs.h>
#include <Common/re2.h>

#include <algorithm>
#include <memory>
#include <vector>

namespace DB
{

namespace
{

/// Splits a path into its non-empty components separated by '/'.
std::vector<std::string> splitPathComponents(const std::string & path)
{
    std::vector<std::string> parts;
    size_t pos = 0;
    while (pos < path.size())
    {
        size_t next = path.find('/', pos);
        if (next == std::string::npos)
            next = path.size();
        if (next > pos)
            parts.emplace_back(path.substr(pos, next - pos));
        pos = next + 1;
    }
    return parts;
}

}

bool globSelectorSpansPathComponents(const std::string & glob_path)
{
    for (const auto & segment : splitPathComponents(glob_path))
    {
        if (std::count(segment.begin(), segment.end(), '{') != std::count(segment.begin(), segment.end(), '}'))
            return true;
    }
    return false;
}

bool globPathHasRecursiveWildcard(const std::string & glob_path)
{
    return glob_path.contains("**");
}

std::function<bool(const std::string &)> makeShouldDescendPredicate(const std::string & glob_path)
{
    auto glob_segments = splitPathComponents(glob_path);

    /// If a '{...}' selector spans a '/', the per-component split is not meaningful: descend always.
    /// Callers must not enable the parallel walk for such globs (see `globSelectorSpansPathComponents`) —
    /// an unconditional descend degrades to one listing request per directory; this is only a safety net.
    if (globSelectorSpansPathComponents(glob_path))
        return [](const std::string &) { return true; };

    if (glob_segments.empty())
        return [](const std::string &) { return true; };

    auto segment_matchers = std::make_shared<std::vector<std::shared_ptr<const re2::RE2>>>();
    segment_matchers->reserve(glob_segments.size());
    for (const auto & segment : glob_segments)
    {
        auto re = std::make_shared<const re2::RE2>(makeRegexpPatternFromGlobs(segment));
        if (!re->ok())
            return [](const std::string &) { return true; };
        segment_matchers->push_back(std::move(re));
    }

    const size_t num_segments = glob_segments.size();

    /// A common prefix always ends with '/'. When `glob_path` itself ends with '/', the keys it matches
    /// are "directory marker" objects (e.g. `root/dir/`): S3 returns such a key only as a `CommonPrefixes`
    /// entry when its parent is listed, and as a `Contents` entry when the marker's own prefix is listed.
    /// Such a marker has exactly `num_segments` components, so to surface it we must descend one extra
    /// level even though there is no room below for a separate file-name segment.
    const bool glob_matches_trailing_slash = !glob_path.empty() && glob_path.back() == '/';

    return [segment_matchers, num_segments, glob_matches_trailing_slash](const std::string & common_prefix) -> bool
    {
        auto components = splitPathComponents(common_prefix);
        const size_t depth = components.size();

        /// A matching key has exactly `num_segments` components. A directory at `depth` levels holds keys
        /// with at least `depth + 1` components, so there must be room below it (depth < num_segments) —
        /// unless the directory marker at exactly `num_segments` is itself a matching key, which happens
        /// only for trailing-slash globs. Anything deeper than `num_segments` can never match.
        if (depth > num_segments)
            return false;
        if (depth == num_segments && !glob_matches_trailing_slash)
            return false;

        for (size_t i = 0; i < depth; ++i)
        {
            if (!re2::RE2::FullMatch(components[i], *(*segment_matchers)[i]))
                return false;
        }
        return true;
    };
}

std::function<bool(const std::string &)> makeIsMarkerOnlyPrefixPredicate(const std::string & glob_path)
{
    /// Only a trailing-slash glob matches directory markers at all; for any other glob the walk descends
    /// into a directory to look for keys *below* it, so no directory is terminal.
    if (glob_path.empty() || glob_path.back() != '/')
        return [](const std::string &) { return false; };

    const size_t num_segments = splitPathComponents(glob_path).size();
    return [num_segments](const std::string & common_prefix)
    {
        return splitPathComponents(common_prefix).size() == num_segments;
    };
}

std::optional<std::string> chooseDelimitedListingStartPrefix(
    const std::string & glob_path,
    const std::string & key_prefix,
    const std::function<bool(const std::string & prefix)> & is_prefix_allowed,
    const std::function<std::vector<std::string>(const std::string & widened_prefix)> & list_widened_level_sample)
{
    if (is_prefix_allowed(key_prefix))
        return key_prefix;

    /// Back the prefix up to the closest '/' boundary at or before its end (the bucket root when the
    /// prefix holds no '/' at all).
    const size_t last_slash = key_prefix.rfind('/');
    std::string widened_prefix = last_slash == std::string::npos ? std::string{} : key_prefix.substr(0, last_slash + 1);

    if (!is_prefix_allowed(widened_prefix))
        return std::nullopt;

    /// The walk parallelizes over the "directory" levels below its start prefix, so it needs at least one
    /// in addition to the segment matching the keys themselves. For a trailing-slash glob the final segment
    /// names the matching directory markers and is itself such a level (`makeShouldDescendPredicate`
    /// descends one extra level to surface the markers), so one segment below the widened prefix is enough;
    /// otherwise the file-name segment does not count and a separate directory segment must remain.
    const size_t min_segments_below = glob_path.ends_with('/') ? 1 : 2;
    if (splitPathComponents(widened_prefix).size() + min_segments_below > splitPathComponents(glob_path).size())
        return std::nullopt;

    /// Loose objects are not pruned, while common prefixes are pruned only after their containing page was
    /// fetched; either kind before the fixed prefix makes the widened walk strictly worse. Sample the
    /// widened level's first page (its lexicographically first entries, so leading junk shows up here if
    /// anywhere) and keep such shapes serial. This is the only check that costs a listing request, so it
    /// comes last. Entries sorting after the region on later pages are cut off at listing time by the root
    /// range's `leastKeyAfterPrefixRegion` bound instead.
    for (const auto & key : list_widened_level_sample(widened_prefix))
    {
        if (!key.starts_with(key_prefix))
            return std::nullopt;
    }

    return widened_prefix;
}

std::optional<std::string> leastKeyAfterPrefixRegion(const std::string & prefix)
{
    std::string bound = prefix;
    while (!bound.empty())
    {
        if (static_cast<unsigned char>(bound.back()) != 0xff)
        {
            bound.back() = static_cast<char>(static_cast<unsigned char>(bound.back()) + 1);
            return bound;
        }
        /// A trailing 0xFF byte admits no successor at its position: every key of the region equals the
        /// prefix up to here and anything greater at this position leaves the region only together with
        /// the shorter position handled by the next iteration.
        bound.pop_back();
    }
    return std::nullopt;
}

}
