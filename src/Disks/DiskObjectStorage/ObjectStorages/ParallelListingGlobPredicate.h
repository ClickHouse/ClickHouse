#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

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
/// For a trailing-slash glob (`root/year=*/`) the final segment names the matching directory markers and
/// counts as such a level itself: the predicate descends one extra level to surface the markers.
///
/// The widened level itself is walked too, and pruning applies only to the sub-"directories" (common
/// prefixes) it exposes: the *loose objects* directly under the widened prefix are all listed, while the
/// narrower serial listing (`Prefix = key_prefix`) never fetches the ones outside `key_prefix`'s key
/// region. A bucket with many such objects (e.g. loose files next to a Hive-style tree) would make the
/// widened walk page through all of them — strictly worse than staying serial. So before committing to a
/// widening, `list_loose_objects_sample(widened_prefix)` is called — exactly once, and only when the
/// widening is otherwise chosen — with the keys of the loose objects (`Contents`, not common prefixes) on
/// the *first* delimited page of the widened prefix; any sampled key outside `key_prefix`'s region keeps
/// the glob on the serial iterator. Loose objects sorting *after* the region that hide beyond the sampled
/// page are handled at listing time instead: the walk's root range is bounded by
/// `leastKeyAfterPrefixRegion(key_prefix)`, so pagination stops once past the last possibly-matching key.
std::optional<std::string> chooseDelimitedListingStartPrefix(
    const std::string & glob_path,
    const std::string & key_prefix,
    const std::function<bool(const std::string & prefix)> & is_prefix_allowed,
    const std::function<std::vector<std::string>(const std::string & widened_prefix)> & list_loose_objects_sample);

/// The least key that sorts after every key starting with `prefix` — the exclusive upper bound of the
/// prefix's key region — or `std::nullopt` when no such key exists (`prefix` is empty or all 0xFF bytes).
/// Used to bound a listing that starts from a prefix *wider* than `prefix` (see
/// `chooseDelimitedListingStartPrefix`) to the region the narrower listing would have covered.
std::optional<std::string> leastKeyAfterPrefixRegion(const std::string & prefix);

}
