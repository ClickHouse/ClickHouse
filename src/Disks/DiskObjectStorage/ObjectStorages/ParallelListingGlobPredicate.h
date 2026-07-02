#pragma once

#include <functional>
#include <string>

namespace DB
{

/// Builds the predicate used by the parallel listing walk (`ObjectStorageParallelListingIterator`) to
/// decide whether a discovered "directory" (a common prefix, always ending with '/') can possibly
/// contain — or itself be — a key matching `glob_path`, so that whole non-matching subtrees are pruned
/// instead of listed.
///
/// Each '/'-separated glob segment is compiled into a matcher for a single path component (glob
/// wildcards '*'/'?' never cross '/'). A directory at depth d is descended into iff each of its d
/// components matches the corresponding glob segment and either there is room below it for the file-name
/// segment (d < number of glob segments), or the directory is itself a matching key (a "directory
/// marker" whose key ends with '/', possible only when `glob_path` ends with '/').
///
/// The predicate is intentionally conservative: it returns `true` whenever it cannot be sure a
/// directory is irrelevant (e.g. a '{...}' selector that spans a '/'), because the per-file regexp
/// `FullMatch` in `nextUnlocked` still guarantees that only truly matching keys are emitted.
/// `glob_path` must not contain the recursive wildcard "**".
std::function<bool(const std::string & common_prefix)> makeShouldDescendPredicate(const std::string & glob_path);

}
