#pragma once

#include <functional>
#include <optional>
#include <string>

namespace DB
{

/// Returns true when a '{...}' selector in `glob_path` spans a '/' (e.g. `root/{a/b,c/d}/*.csv`), i.e.
/// some '/'-separated segment has unbalanced braces. Such globs cannot be pruned per path component, so
/// the predicate built by `makeShouldDescendPredicate` degrades to an unconditional descend — one listing
/// request per directory. Callers must keep such globs on the serial iterator, like the recursive
/// wildcard "**".
bool globSelectorSpansPathComponents(const std::string & glob_path);

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

/// Chooses the prefix the parallel delimiter walk of `glob_path` starts from, or `std::nullopt` when it
/// must stay on the serial iterator. `key_prefix` is the glob's fixed prefix (`cutGlobs`) and
/// `is_prefix_allowed` answers whether the storage accepts a delimited listing that starts from a given
/// prefix (`IObjectStorage::supportsDelimitedListingFromPrefix`).
///
/// Endpoints that accept a `Delimiter` only for '/'-aligned prefixes (S3 Express / directory buckets)
/// cannot start the walk from a prefix that ends mid-component, e.g. `data/year=` for
/// `data/year=*/month=*/data.csv`. Rather than giving up the parallel walk for every such glob, the walk
/// then starts one '/' boundary earlier (`data/`): the walk is a superset of the narrower one, and the
/// predicate of `makeShouldDescendPredicate` prunes the sibling subtrees the wider prefix exposes.
///
/// Backing up only pays off while at least one "directory" level remains below the widened prefix. With
/// none left (e.g. `data_??.csv`, whose prefix `data_` widens to the bucket root), the walk would list a
/// single flat range — serially on exactly the endpoints that need the widening, since they also reject
/// the keyspace split — over a wider prefix than the serial iterator uses, so the serial iterator wins.
std::optional<std::string> chooseDelimitedListingStartPrefix(
    const std::string & glob_path,
    const std::string & key_prefix,
    const std::function<bool(const std::string & prefix)> & is_prefix_allowed);

}
