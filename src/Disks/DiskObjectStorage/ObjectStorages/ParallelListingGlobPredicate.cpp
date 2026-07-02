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

std::function<bool(const std::string &)> makeShouldDescendPredicate(const std::string & glob_path)
{
    auto glob_segments = splitPathComponents(glob_path);

    /// If a '{...}' selector spans a '/', the per-component split is not meaningful: descend always.
    for (const auto & segment : glob_segments)
    {
        if (std::count(segment.begin(), segment.end(), '{') != std::count(segment.begin(), segment.end(), '}'))
            return [](const std::string &) { return true; };
    }

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

}
