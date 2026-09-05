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

/// Returns true when `glob_path` contains `**`. Legacy glob patterns use `**` forms such as
/// `path/**.parquet` and `key=**.parquet` for recursive matching. The parallel delimiter walk can only
/// safely prune globs whose wildcards are confined to individual path components, so callers keep every
/// `**` form on the serial iterator.
bool globPathHasRecursiveWildcard(const std::string & glob_path);

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
/// `glob_path` must not contain `**`.
std::function<bool(const std::string & common_prefix)> makeShouldDescendPredicate(const std::string & glob_path);

/// Builds the predicate telling whether a "directory" the walk descends into is *terminal*: the only key
/// under it that `glob_path` can match is its own directory-marker object (the key equal to the prefix
/// itself). This happens only for a trailing-slash glob (`root/*/`), whose matching keys are the markers:
/// `makeShouldDescendPredicate` descends one extra level into `root/dir/` precisely to surface the marker,
/// which `S3` reports as a `CommonPrefixes` entry of the parent and as a `Contents` entry of the prefix's
/// own listing.
///
/// A marker sorts before every other key under the prefix (it is their strict prefix), so the first page of
/// `ListObjectsV2(Prefix = 'root/dir/', Delimiter = '/')` either returns it or proves it absent. Everything
/// after that page — further pages, and the keyspace split of a file-only directory — can only produce keys
/// that cannot match, so a caller must stop the range there: without this, a marker-less layout such as
/// `root/dNNN/file.csv` would scan every subtree in full just to prove the markers are absent, which is far
/// more requests than the serial listing needs.
std::function<bool(const std::string & common_prefix)> makeIsMarkerOnlyPrefixPredicate(const std::string & glob_path);

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
/// The widened level itself is walked too. Loose objects directly under it are all listed, while common
/// prefixes are pruned only after their containing page was fetched. Either kind outside `key_prefix`'s
/// region makes the widened walk page through entries the narrower serial listing (`Prefix = key_prefix`)
/// never fetches — strictly worse than staying serial. So before committing to a widening,
/// `list_widened_level_sample(widened_prefix)` is called — exactly once, and only when the widening is
/// otherwise chosen — with the `Contents` and `CommonPrefixes` of the *first* delimited page; any sampled
/// entry outside `key_prefix`'s region keeps the glob on the serial iterator. Entries sorting *after* the
/// region that hide beyond the sampled page are handled at listing time instead: the walk's root range is
/// bounded by
/// `leastKeyAfterPrefixRegion(key_prefix)`, so pagination stops once past the last possibly-matching key.
std::optional<std::string> chooseDelimitedListingStartPrefix(
    const std::string & glob_path,
    const std::string & key_prefix,
    const std::function<bool(const std::string & prefix)> & is_prefix_allowed,
    const std::function<std::vector<std::string>(const std::string & widened_prefix)> & list_widened_level_sample);

/// The least key that sorts after every key starting with `prefix` — the exclusive upper bound of the
/// prefix's key region — or `std::nullopt` when no such key exists (`prefix` is empty or all 0xFF bytes).
/// Used to bound a listing that starts from a prefix *wider* than `prefix` (see
/// `chooseDelimitedListingStartPrefix`) to the region the narrower listing would have covered.
std::optional<std::string> leastKeyAfterPrefixRegion(const std::string & prefix);

}
