#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageParallelListingIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ParallelListingGlobPredicate.h>
#include <Common/CurrentMetrics.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>

#include <atomic>
#include <algorithm>
#include <cstdint>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/format.h>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric ObjectStorageParallelListingThreadsScheduled;
}

namespace
{

/// A faithful in-memory emulation of S3 `ListObjectsV2`: holds a sorted, unique set of keys and
/// answers one page at a time honoring Prefix, Delimiter (common-prefix grouping), StartAfter,
/// ContinuationToken and MaxKeys, reporting truncation. Used to drive the parallel iterator without a
/// real object storage and to assert that every key is produced exactly once.
struct FakeS3
{
    std::vector<std::string> keys; /// sorted, unique
    size_t page_size = 1000;
    mutable std::atomic<size_t> requests{0};
    /// Requests that used a non-empty `StartAfter`: the keyspace split issues these to tile the keyspace,
    /// and `StartAfter` is unsupported by S3 Express / directory buckets, so the gated (no-keyspace-split)
    /// path must avoid it. `requests_with_empty_delimiter` tracks requests that dropped the '/' delimiter:
    /// the split stays '/'-delimited (so sub-directories are discovered and pruned), so this must stay zero.
    mutable std::atomic<size_t> requests_with_start_after{0};
    mutable std::atomic<size_t> requests_with_empty_delimiter{0};

    void add(std::string key) { keys.push_back(std::move(key)); }

    void finalize()
    {
        std::sort(keys.begin(), keys.end());
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    }

    ObjectStorageListResult list(
        const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & token) const
    {
        requests.fetch_add(1, std::memory_order_relaxed);
        if (!start_after.empty())
            requests_with_start_after.fetch_add(1, std::memory_order_relaxed);
        if (delimiter.empty())
            requests_with_empty_delimiter.fetch_add(1, std::memory_order_relaxed);

        /// The continuation token is opaque in real `ListObjectsV2`, so its meaning cannot be recovered
        /// from the marker text: a real object key can itself end with the delimiter (a directory-marker
        /// object like `root/dir/`). The fake therefore encodes the kind in the token it mints below —
        /// "P:<prefix>" = the page ended on a `CommonPrefixes` entry (resume after the whole group),
        /// "O:<key>" = it ended on a `Contents` key (resume right after that single key, keeping the
        /// keys below a same-named directory marker).
        std::string marker;
        bool marker_is_group = false;
        if (!token.empty())
        {
            if (token.starts_with("P:"))
                marker_is_group = true;
            else if (!token.starts_with("O:"))
                throw std::logic_error("FakeS3: unknown continuation token: " + token);
            marker = token.substr(2);
        }
        else
        {
            /// `StartAfter` is a plain key with no group meaning (as in real S3): if remaining keys fall
            /// under a common prefix equal to it, that group is emitted afresh, not skipped.
            marker = start_after;
        }

        ObjectStorageListResult res;
        size_t count = 0;
        std::string last_group;
        std::string last_token;

        for (const auto & key : keys)
        {
            if (!key.starts_with(prefix))
                continue;
            if (!marker.empty())
            {
                if (key <= marker)
                    continue;
                if (marker_is_group && key.starts_with(marker))
                    continue; /// already covered by the previously emitted common prefix
            }

            if (!delimiter.empty())
            {
                const size_t dpos = key.find(delimiter, prefix.size());
                if (dpos != std::string::npos)
                {
                    std::string cp = key.substr(0, dpos + delimiter.size());
                    if (cp == last_group)
                        continue;
                    if (count >= page_size)
                    {
                        res.is_truncated = true;
                        res.next_continuation_token = last_token;
                        return res;
                    }
                    res.common_prefixes.push_back(cp);
                    last_group = cp;
                    last_token = "P:" + std::move(cp);
                    ++count;
                    continue;
                }
            }

            if (count >= page_size)
            {
                res.is_truncated = true;
                res.next_continuation_token = last_token;
                return res;
            }
            res.objects.push_back(std::make_shared<RelativePathWithMetadata>(key));
            last_token = "O:" + key;
            ++count;
        }
        return res;
    }
};

/// A well-distributed `digits`-hex-digit name derived from `i` (splitmix64). A plain LCG sampled at its
/// low bits (`hex[x & 0xf]`) is unusable here: the low nibble of an LCG cycles with a tiny period that does
/// not depend on the seed's high bits, so all `i` sharing `i mod 16` collapse to one name (~16 distinct
/// keys total) and a flat directory is never actually split. This yields ~`n` distinct keys with a uniform
/// first byte, so keyspace splitting is genuinely exercised. `digits` must be <= 16.
std::string hexName(uint64_t i, size_t digits = 16)
{
    uint64_t z = i + 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z = z ^ (z >> 31);
    static constexpr char hex[] = "0123456789abcdef";
    std::string s;
    s.reserve(digits);
    for (size_t c = 0; c < digits; ++c)
        s.push_back(hex[(z >> (c * 4)) & 0xf]);
    return s;
}

ObjectStorageParallelListingIterator::ListLevelFunction makeListLevel(const FakeS3 & s3)
{
    return [&s3](const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & token)
    { return s3.list(prefix, delimiter, start_after, token); };
}

/// The lightweight existence probe the flat keyspace split uses (what production wires as `with_tags=false`,
/// `MaxKeys=1`). Emulating `MaxKeys=1` here — returning only the single globally smallest entry — also
/// asserts that one key is enough for the probe to drive a correct, complete flat split.
ObjectStorageParallelListingIterator::ProbeLevelFunction makeProbeLevel(const FakeS3 & s3)
{
    return [&s3](const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & token)
    {
        auto res = s3.list(prefix, delimiter, start_after, token);
        const bool has_object = !res.objects.empty();
        const bool has_prefix = !res.common_prefixes.empty();
        /// Keep the single smallest entry (object vs. common prefix), like `ListObjectsV2` with MaxKeys=1.
        if (has_object && (!has_prefix || res.objects.front()->getPath() <= res.common_prefixes.front()))
            res.common_prefixes.clear();
        else if (has_prefix)
            res.objects.clear();
        if (res.objects.size() > 1)
            res.objects.resize(1);
        if (res.common_prefixes.size() > 1)
            res.common_prefixes.resize(1);
        return res;
    };
}

std::vector<std::string> drain(ObjectStorageParallelListingIterator & iterator)
{
    std::vector<std::string> result;
    while (auto batch = iterator.getCurrentBatchAndScheduleNext())
        for (const auto & object : *batch)
            result.push_back(object->relative_path);
    return result;
}

/// All keys under `prefix`, sorted (the full set the iterator must produce).
std::vector<std::string> expectedUnder(const FakeS3 & s3, const std::string & prefix)
{
    std::vector<std::string> e;
    for (const auto & k : s3.keys)
        if (k.starts_with(prefix))
            e.push_back(k);
    std::sort(e.begin(), e.end());
    return e;
}

/// Serially paginate the fake exactly as a plain (non-parallel) `ListObjectsV2` consumer would,
/// following continuation tokens, and collect the objects and common prefixes across all pages.
std::pair<std::vector<std::string>, std::vector<std::string>> paginateSerially(
    const FakeS3 & s3, const std::string & prefix, const std::string & delimiter)
{
    std::vector<std::string> objects;
    std::vector<std::string> prefixes;
    std::string token;
    while (true)
    {
        auto res = s3.list(prefix, delimiter, /* start_after */ "", token);
        for (const auto & object : res.objects)
            objects.push_back(object->relative_path);
        for (const auto & cp : res.common_prefixes)
            prefixes.push_back(cp);
        if (!res.is_truncated)
            break;
        token = res.next_continuation_token;
    }
    return {objects, prefixes};
}

auto descendAll = [](const std::string &) { return true; };

/// Runs the iterator over `prefix` at several parallelism levels and asserts the produced key set is
/// exactly `expected` every time (complete, no duplicates), regardless of thread scheduling.
void assertCompleteForAllParallelism(const FakeS3 & s3, const std::string & prefix, const std::vector<std::string> & expected)
{
    for (size_t threads : {1, 2, 4, 16, 64})
    {
        ObjectStorageParallelListingIterator iterator(
            prefix, threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), descendAll);
        auto got = drain(iterator);
        std::sort(got.begin(), got.end());
        EXPECT_EQ(got, expected) << "threads=" << threads << " prefix=" << prefix;
    }
}

}

TEST(ObjectStorageParallelListing, FlatBigDirectoryUUIDs)
{
    /// ~6000 UUID-like (hex) keys in one flat directory, page size small so it is heavily truncated and
    /// must be split by keyspace. Mimics musicbrainz/mlhdplus-complete.
    FakeS3 s3;
    s3.page_size = 50;
    for (size_t i = 0; i < 6000; ++i)
        s3.add("mb/flat/" + hexName(i) + ".txt.zst");
    s3.finalize();

    assertCompleteForAllParallelism(s3, "mb/flat/", expectedUnder(s3, "mb/flat/"));
}

TEST(ObjectStorageParallelListing, KeyspaceSplitCanBeDisabledForDirectoryBuckets)
{
    /// S3 Express / directory buckets reject `StartAfter`, so for them keyspace splitting is disabled. The
    /// same big flat directory must still be listed completely, but only via serial pagination — never
    /// issuing a `StartAfter` request — while the hierarchical delimiter walk (used here only at the root)
    /// stays available. (The split itself always keeps the '/' delimiter, so no request ever drops it,
    /// whether the split is enabled or not.)
    auto fill = [](FakeS3 & s3)
    {
        for (size_t i = 0; i < 6000; ++i)
            s3.add("mb/flat/" + hexName(i) + ".txt.zst");
        s3.finalize();
    };

    std::vector<std::string> expected;
    {
        FakeS3 s3;
        fill(s3);
        expected = expectedUnder(s3, "mb/flat/");
    }

    /// Sanity check the gate is meaningful: with splitting enabled, this uniform directory IS split, so
    /// `StartAfter` requests are indeed issued (while the '/' delimiter is always kept).
    {
        FakeS3 s3;
        s3.page_size = 50;
        fill(s3);
        ObjectStorageParallelListingIterator iterator(
            "mb/flat/", 16, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), descendAll,
            /* allow_keyspace_split */ true);
        auto got = drain(iterator);
        std::sort(got.begin(), got.end());
        EXPECT_EQ(got, expected);
        EXPECT_GT(s3.requests_with_start_after.load(), 0u);
        EXPECT_EQ(s3.requests_with_empty_delimiter.load(), 0u);
    }

    /// With splitting disabled, the listing is still complete, but no `StartAfter` and no empty delimiter
    /// are ever sent, regardless of the requested parallelism.
    for (size_t threads : {1, 4, 16})
    {
        FakeS3 s3;
        s3.page_size = 50;
        fill(s3);
        ObjectStorageParallelListingIterator iterator(
            "mb/flat/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), descendAll,
            /* allow_keyspace_split */ false);
        auto got = drain(iterator);
        std::sort(got.begin(), got.end());
        EXPECT_EQ(got, expected) << "threads=" << threads;
        EXPECT_EQ(s3.requests_with_start_after.load(), 0u) << "threads=" << threads;
        EXPECT_EQ(s3.requests_with_empty_delimiter.load(), 0u) << "threads=" << threads;
    }
}

TEST(ObjectStorageParallelListing, FlatSplitProbeUsesDedicatedCallback)
{
    /// Regression test: the flat keyspace-split existence probe must go through the dedicated probe callback,
    /// not the main `list_level`. In production the probe callback lists a single key with `with_tags=false`,
    /// so enabling `s3_list_object_parallelism` on a `_tags` scan must not turn the split probe into a fan of
    /// redundant `GetObjectTagging` requests for a page it discards. Here the two callbacks get separate
    /// counters, and the probe returns only one key, so a green run proves both that the split is driven by
    /// the dedicated probe and that a single-key probe is enough to keep the listing complete.
    FakeS3 s3;
    s3.page_size = 50;
    for (size_t i = 0; i < 6000; ++i)
        s3.add("mb/flat/" + hexName(i) + ".txt.zst");
    s3.finalize();

    std::atomic<size_t> probe_calls{0};
    auto base_probe = makeProbeLevel(s3);
    auto probe_level = [&probe_calls, base_probe](
        const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & token)
    {
        probe_calls.fetch_add(1, std::memory_order_relaxed);
        return base_probe(prefix, delimiter, start_after, token);
    };

    ObjectStorageParallelListingIterator iterator(
        "mb/flat/", 16, /* max_buffered_keys */ 256, makeListLevel(s3), probe_level, descendAll,
        /* allow_keyspace_split */ true);
    auto got = drain(iterator);
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, expectedUnder(s3, "mb/flat/"));
    /// This uniform flat directory is split, so the dedicated (tags-free, single-key) probe was exercised.
    EXPECT_GT(probe_calls.load(), 0u);
}

TEST(ObjectStorageParallelListing, GapsOutsideSampledAlphabet)
{
    /// Mostly hex keys, plus a handful whose first byte after the prefix is OUTSIDE the hex alphabet
    /// (punctuation, uppercase, and a high byte). These bytes never appear on the hex pages, so a naive
    /// alphabet-bucket split would miss them; contiguous range-tiling must still cover them.
    FakeS3 s3;
    s3.page_size = 40;
    for (size_t i = 0; i < 3000; ++i)
        s3.add("p/" + hexName(i, 12));
    /// Keys with first-byte well outside the hex range, spread across the byte space.
    for (char weird : {'!', '%', '-', '.', 'A', 'Z', '_', '~'})
        for (int j = 0; j < 5; ++j)
            s3.add(std::string("p/") + weird + "weird" + std::to_string(j));
    s3.add(std::string("p/") + '\x01' + "low");
    s3.add(std::string("p/") + '\xfe' + "high");
    s3.add(std::string("p/") + '\xff' + "highest");
    s3.finalize();

    assertCompleteForAllParallelism(s3, "p/", expectedUnder(s3, "p/"));
}

TEST(ObjectStorageParallelListing, SharedLongPrefixThenDiverge)
{
    /// All keys share a long common prefix beyond the directory before diverging — the split position
    /// must advance to where keys actually differ.
    FakeS3 s3;
    s3.page_size = 30;
    for (size_t i = 0; i < 2000; ++i)
        s3.add(fmt::format("d/common_prefix_part_{:05}", i));
    s3.finalize();

    assertCompleteForAllParallelism(s3, "d/", expectedUnder(s3, "d/"));
}

TEST(ObjectStorageParallelListing, BoundaryKeysExactlyOnePastPrefix)
{
    /// Keys that are exactly prefix+one byte (the split boundaries themselves), mixed with longer keys,
    /// to exercise the inclusive-end / exclusive-start handling so boundary keys are produced once.
    FakeS3 s3;
    s3.page_size = 5;
    for (char c = 'a'; c <= 'z'; ++c)
    {
        s3.add(std::string("k/") + c);            /// exactly prefix + one byte
        s3.add(std::string("k/") + c + "tail");   /// a longer key in the same bucket
        s3.add(std::string("k/") + c + c + "x");
    }
    s3.finalize();

    assertCompleteForAllParallelism(s3, "k/", expectedUnder(s3, "k/"));
}

TEST(ObjectStorageParallelListing, SinglePageNotSplit)
{
    FakeS3 s3;
    s3.page_size = 1000;
    for (int i = 0; i < 100; ++i)
        s3.add("s/" + std::to_string(i));
    s3.finalize();

    ObjectStorageParallelListingIterator iterator("s/", 8, 1000, makeListLevel(s3), makeProbeLevel(s3), descendAll);
    auto got = drain(iterator);
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, expectedUnder(s3, "s/"));
    /// A directory that fits in one page must not be split into many requests.
    EXPECT_LE(s3.requests.load(), 2u);
}

TEST(ObjectStorageParallelListing, HierarchicalTree)
{
    /// Hierarchical (Hive-style) layout listed via the '/' delimiter.
    FakeS3 s3;
    s3.page_size = 100;
    for (int y = 2020; y <= 2023; ++y)
        for (int m = 1; m <= 12; ++m)
            for (int f = 0; f < 30; ++f)
                s3.add(fmt::format("root/year={}/month={:02}/data_{:03}.parquet", y, m, f));
    s3.finalize();

    assertCompleteForAllParallelism(s3, "root/", expectedUnder(s3, "root/"));
}

TEST(ObjectStorageParallelListing, PendingRangeFrontierStaysBoundedOnWideTree)
{
    /// Regression test: a wide, deep hierarchical layout whose interior pages return only common prefixes
    /// (no leaf objects until the deepest level) must not let the pending-range frontier grow with the total
    /// directory count. The buffered-object backpressure never engages on those directory-only pages (there
    /// are no leaf objects for the consumer to drain), so a breadth-first walk would accumulate one pending
    /// range per directory — here ~8000 — and burn per-query memory unrelated to `s3_list_object_keys_size`.
    /// The depth-first walk keeps the frontier bounded by the active parallelism and the tree depth instead.
    FakeS3 s3;
    s3.page_size = 100;
    constexpr int width = 20; /// 20 x 20 x 20 leaf directories, one file each.
    for (int a = 0; a < width; ++a)
        for (int b = 0; b < width; ++b)
            for (int c = 0; c < width; ++c)
                s3.add(fmt::format("t/a={:02}/b={:02}/c={:02}/f.dat", a, b, c));
    s3.finalize();

    /// 20 + 400 + 8000 = 8420 directories in total; a breadth-first walk would reach ~8000 pending ranges.
    const size_t total_directories = width + (width * width) + (width * width * width);

    for (size_t threads : {2, 8, 16})
    {
        ObjectStorageParallelListingIterator iterator(
            "t/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), descendAll);
        auto got = drain(iterator);
        std::sort(got.begin(), got.end());
        EXPECT_EQ(got, expectedUnder(s3, "t/")) << "threads=" << threads;

        /// The frontier stays far below the total directory count (bounded by ~threads * depth * fan-out),
        /// not the O(total_directories) a breadth-first walk would reach.
        const size_t peak = iterator.getPeakOutstandingRanges();
        EXPECT_LT(peak, total_directories / 3)
            << "pending-range frontier grew to " << peak << " of " << total_directories
            << " directories (threads=" << threads << ")";
    }
}

TEST(ObjectStorageParallelListing, PaginatedParentDirectoryFrontierStaysBounded)
{
    /// Regression test: a single directory whose common prefixes span *many pages* (far more immediate
    /// sub-directories than fit in one listing page), with no leaf objects until a deeper level, must not
    /// let the pending-range frontier grow with the number of siblings. Fully paginating that parent in
    /// place before descending would append one child range per sub-directory to the frontier — here
    /// ~8000 — reproducing the very `O(total_directories)` memory blow-up the depth-first walk is meant to
    /// remove; the earlier `PendingRangeFrontierStaysBoundedOnWideTree` fixture misses it because each of
    /// its directories fits in a single page. Re-enqueuing the parent's continuation as its own range keeps
    /// the frontier bounded to at most one page of siblings per active worker instead.
    FakeS3 s3;
    s3.page_size = 50;
    constexpr int width = 8000; /// one directory with 8000 immediate sub-directories = 160 pages of prefixes
    for (int a = 0; a < width; ++a)
        s3.add(fmt::format("w/d={:05}/f.dat", a)); /// the only leaf object lives one level below `w/`
    s3.finalize();

    /// The single wide directory has `width` immediate sub-directories, one leaf file each; a walk that
    /// paginates it in place would reach ~`width` pending ranges.
    const size_t total_directories = width;

    for (size_t threads : {1, 4, 16})
    {
        ObjectStorageParallelListingIterator iterator(
            "w/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), descendAll);
        auto got = drain(iterator);
        std::sort(got.begin(), got.end());
        EXPECT_EQ(got, expectedUnder(s3, "w/")) << "threads=" << threads;

        /// Bounded by ~threads * page_size (at most one page of siblings per active worker), which is far
        /// below the `O(total_directories)` an in-place pagination of the parent would reach.
        const size_t peak = iterator.getPeakOutstandingRanges();
        EXPECT_LT(peak, total_directories / 4)
            << "pending-range frontier grew to " << peak << " of " << total_directories
            << " sibling sub-directories (threads=" << threads << ")";
    }
}

TEST(ObjectStorageParallelListing, MixedHierarchicalAndFlat)
{
    /// A '/'-partitioned tree where each leaf directory is itself a big flat directory (needs both
    /// hierarchical descent and keyspace splitting).
    FakeS3 s3;
    s3.page_size = 25;
    for (int p = 0; p < 8; ++p)
        for (size_t i = 0; i < 400; ++i)
            s3.add("m/part=" + std::to_string(p) + "/" + hexName(p * 1000 + i, 10));
    s3.finalize();

    assertCompleteForAllParallelism(s3, "m/", expectedUnder(s3, "m/"));
}

TEST(ObjectStorageParallelListing, Pruning)
{
    FakeS3 s3;
    s3.page_size = 100;
    for (int f = 0; f < 50; ++f)
        s3.add("root/keep/a" + std::to_string(f));
    for (int f = 0; f < 50; ++f)
        s3.add("root/skip/b" + std::to_string(f));
    s3.finalize();

    auto should_descend = [](const std::string & prefix) { return prefix.find("skip") == std::string::npos; };
    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, makeListLevel(s3), makeProbeLevel(s3), should_descend);
    auto got = drain(iterator);
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, expectedUnder(s3, "root/keep/"));
}

TEST(ObjectStorageParallelListing, MixedPrefixLooksFlatThenExposesSubdirectory)
{
    /// Regression test for a performance-contract violation of the flat keyspace split: a mixed prefix
    /// whose *first* truncated page contains only flat files (so it looks flat) but which exposes a
    /// sub-directory on a *later* page must not be keyspace-split in a way that scans that sub-tree
    /// recursively — the split must stay '/'-delimited so the sub-directory surfaces as a common prefix
    /// and `should_descend` prunes it, exactly as the plain hierarchical walk would.
    ///
    /// Here `root/` holds many `NNNNNNN.csv` files (first byte a digit, so they sort before `z`) plus a
    /// `root/zsub/` directory with its own large sub-tree. The small page size makes the first page all
    /// digit-named files (no common prefix) and truncated, so the flat split is attempted; a `should_descend`
    /// that prunes `zsub` (as the glob `root/*.csv` would) must keep the whole `zsub` sub-tree unlisted.
    FakeS3 s3;
    s3.page_size = 50;
    for (int i = 0; i < 3000; ++i)
        s3.add(fmt::format("root/{:07}.csv", i));
    for (int i = 0; i < 3000; ++i)
        s3.add(fmt::format("root/zsub/{:07}.bin", i));
    s3.finalize();

    auto should_descend = [](const std::string & prefix) { return prefix.find("zsub") == std::string::npos; };

    std::vector<std::string> expected_csvs;
    for (const auto & key : s3.keys)
        if (key.find("zsub") == std::string::npos)
            expected_csvs.push_back(key);
    std::sort(expected_csvs.begin(), expected_csvs.end());

    for (size_t threads : {1, 2, 4, 16})
    {
        ObjectStorageParallelListingIterator iterator(
            "root/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), should_descend,
            /* allow_keyspace_split */ true);
        auto listed = drain(iterator);

        /// The pruned `zsub` sub-tree must never be scanned (that is the "much larger scan" the split
        /// would otherwise turn an opt-in speedup into).
        for (const auto & key : listed)
            EXPECT_EQ(key.find("zsub"), std::string::npos) << "scanned pruned sub-tree: " << key << " threads=" << threads;

        /// The flat files must still all be produced exactly once.
        std::vector<std::string> csvs = listed;
        std::sort(csvs.begin(), csvs.end());
        EXPECT_EQ(csvs, expected_csvs) << "threads=" << threads;
    }
}

TEST(ObjectStorageParallelListing, DirectoryMarkerMatchesTrailingSlashGlob)
{
    /// A "directory marker" object whose key itself ends with '/' (e.g. `root/dir/`, as created by some
    /// S3 tools). For glob `root/*/` the serial iterator returns the marker (the full regexp matches it),
    /// so the parallel walk driven by the real `makeShouldDescendPredicate` must surface it too: S3 returns
    /// `root/dir/` only as a `CommonPrefixes` entry when listing `root/`, and as a `Contents` entry when its
    /// own prefix is listed, so the predicate must descend into a common prefix that is itself a match.
    FakeS3 s3;
    s3.page_size = 100;
    s3.add("root/dir/");           /// directory-marker object that matches `root/*/`
    s3.add("root/dir/file.csv");   /// a regular file below it (does not match `root/*/`)
    s3.add("root/dir2/");          /// another matching marker
    s3.add("root/other/x.csv");    /// a sibling directory with no marker (does not match)
    s3.finalize();

    const std::string glob = "root/*/";
    const re2::RE2 matcher(makeRegexpPatternFromGlobs(glob));
    ASSERT_TRUE(matcher.ok());

    /// What serial listing yields: every key under the prefix that the full glob regexp accepts.
    std::vector<std::string> expected;
    for (const auto & key : s3.keys)
        if (re2::RE2::FullMatch(key, matcher))
            expected.push_back(key);
    std::sort(expected.begin(), expected.end());
    ASSERT_EQ(expected, (std::vector<std::string>{"root/dir/", "root/dir2/"}));

    for (size_t threads : {1, 2, 4, 16, 64})
    {
        ObjectStorageParallelListingIterator iterator(
            "root/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), makeShouldDescendPredicate(glob));
        auto listed = drain(iterator);

        /// The walk may legitimately emit extra non-matching keys (the downstream per-file matcher drops
        /// them); the invariant is that every glob-matching key is produced exactly once.
        std::vector<std::string> matched;
        for (const auto & key : listed)
            if (re2::RE2::FullMatch(key, matcher))
                matched.push_back(key);
        std::sort(matched.begin(), matched.end());
        EXPECT_EQ(matched, expected) << "threads=" << threads;
    }
}

TEST(ObjectStorageParallelListing, DirectoryMarkerOnTruncatedPageBoundary)
{
    /// Regression test for the fake itself and for the walk over it: a directory-marker object key
    /// (`root/dir/`) landing exactly on a page boundary. The continuation token is opaque in real
    /// `ListObjectsV2`, so "the previous page ended on a common prefix" cannot be reconstructed from the
    /// marker text — `root/dir/` is simultaneously a real object key and the name of a group. With the
    /// kind encoded in the fake token, resuming after the *object* `root/dir/` must keep listing the
    /// keys below it, while resuming after the *common prefix* `root/dir/` must skip the whole group.
    /// `page_size = 1` forces every entry onto its own page, so both resume kinds are exercised.
    FakeS3 s3;
    s3.page_size = 1;
    s3.add("root/dir/");           /// directory-marker object
    s3.add("root/dir/a.csv");
    s3.add("root/dir/b.csv");
    s3.add("root/dir2/");          /// another marker, so a group boundary also lands on a page break
    s3.add("root/dir2/c.csv");
    s3.add("root/eee.csv");        /// a plain sibling file after all the groups
    s3.finalize();

    /// Listing the marker's own prefix returns the marker and its children as `Contents`; ending the
    /// first page on the object `root/dir/` must not skip `root/dir/a.csv` and `root/dir/b.csv`.
    {
        auto [objects, prefixes] = paginateSerially(s3, "root/dir/", "/");
        EXPECT_EQ(objects, (std::vector<std::string>{"root/dir/", "root/dir/a.csv", "root/dir/b.csv"}));
        EXPECT_TRUE(prefixes.empty());
    }

    /// Listing the parent groups each directory into one `CommonPrefixes` entry (the marker never shows
    /// up as `Contents` here, exactly like real S3), and a page ending on a group resumes after it.
    {
        auto [objects, prefixes] = paginateSerially(s3, "root/", "/");
        EXPECT_EQ(objects, (std::vector<std::string>{"root/eee.csv"}));
        EXPECT_EQ(prefixes, (std::vector<std::string>{"root/dir/", "root/dir2/"}));
    }

    /// The parallel walk over this maximally truncated layout must still produce every key exactly once.
    assertCompleteForAllParallelism(s3, "root/", expectedUnder(s3, "root/"));
    assertCompleteForAllParallelism(s3, "root/dir/", expectedUnder(s3, "root/dir/"));

    /// And the glob-driven walk (as in `DirectoryMarkerMatchesTrailingSlashGlob`, but under truncation)
    /// must still surface each matching marker exactly once.
    const std::string glob = "root/*/";
    const re2::RE2 matcher(makeRegexpPatternFromGlobs(glob));
    ASSERT_TRUE(matcher.ok());
    for (size_t threads : {1, 2, 4, 16})
    {
        ObjectStorageParallelListingIterator iterator(
            "root/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeProbeLevel(s3), makeShouldDescendPredicate(glob));
        auto listed = drain(iterator);
        std::vector<std::string> matched;
        for (const auto & key : listed)
            if (re2::RE2::FullMatch(key, matcher))
                matched.push_back(key);
        std::sort(matched.begin(), matched.end());
        EXPECT_EQ(matched, (std::vector<std::string>{"root/dir/", "root/dir2/"})) << "threads=" << threads;
    }
}

TEST(ObjectStorageParallelListing, ExceptionPropagates)
{
    auto list_level = [](const std::string & prefix, const std::string &, const std::string &, const std::string &) -> ObjectStorageListResult
    {
        if (prefix == "root/")
        {
            ObjectStorageListResult result;
            result.common_prefixes = {"root/bad/"};
            return result;
        }
        throw std::runtime_error("listing failed");
    };

    /// No flat split happens here, so the probe callback is never invoked; reuse `list_level` for it.
    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, list_level, list_level, descendAll);
    EXPECT_THROW(drain(iterator), std::runtime_error);
}

TEST(ObjectStorageParallelListing, EmptyResult)
{
    FakeS3 s3;
    s3.finalize();
    ObjectStorageParallelListingIterator iterator("nothing/", 4, 1000, makeListLevel(s3), makeProbeLevel(s3), descendAll);
    EXPECT_TRUE(drain(iterator).empty());
}

TEST(ObjectStorageParallelListing, WorkersStartOnDemandNotEagerly)
{
    /// Regression test: a large `s3_list_object_parallelism` must not reserve the whole clamped worker
    /// count up front. Because the pool is backed by the global thread pool, eagerly scheduling
    /// `num_threads` workers for a listing that needs far fewer would let a single glob iterator grab (and
    /// idle) that many global-pool threads — enough to starve the server. Workers must be started on
    /// demand instead, so a listing with no fan-out runs on a single worker regardless of the requested
    /// parallelism.
    ///
    /// Here the root listing blocks and never returns (so it never discovers a sub-directory to fan out
    /// into): the single root worker parks inside it, and no further range ever appears. On-demand
    /// spawning schedules exactly one worker; the old eager code scheduled `num_threads` of them. We
    /// observe this via the pool's "scheduled jobs" metric, which is bumped synchronously as each worker
    /// is scheduled and only cleared when the worker finishes — and here no worker finishes before the
    /// listing is released.
    std::promise<void> worker_running;      /// fulfilled once the root worker is inside `list_level`
    std::atomic<bool> running_signaled{false};
    std::promise<void> release_listing;     /// fulfilled to let the (simulated) listing request return
    auto release_future = release_listing.get_future().share();

    auto list_level = [&](const std::string &, const std::string &, const std::string &, const std::string &) -> ObjectStorageListResult
    {
        if (!running_signaled.exchange(true))
            worker_running.set_value();
        release_future.wait();
        return {};
    };

    constexpr size_t num_threads = 64;
    const auto scheduled_before = CurrentMetrics::get(CurrentMetrics::ObjectStorageParallelListingThreadsScheduled);

    ObjectStorageParallelListingIterator iterator(
        "root/", num_threads, /* max_buffered_keys */ 256, list_level, list_level, descendAll);

    /// Drive the iterator from a separate thread: draining blocks until the (stalled) listing produces a
    /// batch or finishes.
    std::thread consumer([&] { drain(iterator); });

    /// Deterministic wait (no sleep): once the single worker is parked inside the blocked `list_level`,
    /// its own scheduling is already reflected in the metric and — since nothing fans out — no other
    /// worker is ever scheduled, so the count is stable at one.
    worker_running.get_future().wait();
    EXPECT_EQ(
        CurrentMetrics::get(CurrentMetrics::ObjectStorageParallelListingThreadsScheduled) - scheduled_before, 1)
        << "expected a single on-demand worker for a no-fan-out listing, not the full num_threads="
        << num_threads;

    /// Let the stalled request return so the walk finishes and the consumer thread unblocks.
    release_listing.set_value();
    consumer.join();
}

TEST(ObjectStorageParallelListing, CancellationUnblocksBlockedConsumer)
{
    /// Regression test for the `Hung check` deadlock: while the producers make no progress (a stalled
    /// listing request) the consumer must not block forever — `check_cancellation` is polled and, once it
    /// throws, the wait is interrupted and the exception propagates. Without the fix the consumer would
    /// wait indefinitely (nothing buffered, not finished, not stopped), ignoring query cancellation.
    std::promise<void> entered_listing;     /// fulfilled when the (simulated) listing request starts
    std::atomic<bool> entered_done{false};
    std::promise<void> release_listing;     /// fulfilled to let the (simulated) listing request return
    auto release_future = release_listing.get_future().share();

    auto list_level = [&](const std::string &, const std::string &, const std::string &, const std::string &) -> ObjectStorageListResult
    {
        if (!entered_done.exchange(true))
            entered_listing.set_value();
        /// Simulate a `ListObjectsV2` that stalls and does not return until released, so the consumer is
        /// forced to wait with nothing buffered and the walk not finished.
        release_future.wait();
        return {};
    };

    std::atomic<bool> cancelled{false};
    std::function<void()> check_cancellation = [&]
    {
        if (cancelled.load())
            throw std::runtime_error("query cancelled");
    };

    ObjectStorageParallelListingIterator iterator(
        "root/", 4, /* max_buffered_keys */ 256, list_level, list_level, descendAll,
        /* allow_keyspace_split */ true, std::move(check_cancellation));

    /// Cancel only once a worker is actually parked inside the stalled listing request.
    std::thread canceller([&]
    {
        entered_listing.get_future().wait();
        cancelled.store(true);
    });

    EXPECT_THROW(drain(iterator), std::runtime_error);

    canceller.join();
    /// Let the stalled request return so the iterator can be destroyed without blocking on the worker.
    release_listing.set_value();
}

TEST(ObjectStorageParallelListing, GetCurrentBatchReturnsWithoutWaitingForNextBatch)
{
    /// Regression test: `getCurrentBatchAndScheduleNext` must return the current batch immediately and let
    /// the workers keep filling the next one in the background, rather than blocking on the next batch
    /// before returning the current one. The batching API is how the consumer
    /// (`StorageObjectStorageSource::GlobIterator`) overlaps listing with reading files; if the first batch
    /// were withheld until the next batch is produced, a glob whose first page is followed by a large pruned
    /// subtree would make the first file read wait for the whole remaining walk, re-serializing listing and
    /// reading and erasing the speedup this iterator adds.
    ///
    /// Here `root/` yields a first batch of leaf objects plus one sub-directory `root/sub/` whose listing
    /// stalls until released. The first `getCurrentBatchAndScheduleNext` must return the `root/` leaves
    /// while `root/sub/` is still stalled; the old eager code blocked here until `root/sub/` produced a
    /// batch (i.e. until the stall was released).
    FakeS3 s3;
    s3.add("root/a.dat");
    s3.add("root/b.dat");
    s3.add("root/sub/c.dat");
    s3.finalize();

    std::promise<void> release_sub;         /// fulfilled to let the sub-directory listing return
    auto release_sub_future = release_sub.get_future().share();

    auto list_level = [&](const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & token) -> ObjectStorageListResult
    {
        /// Only the sub-directory listing stalls; the root listing (which produces the first batch) does not.
        if (prefix == "root/sub/")
            release_sub_future.wait();
        return s3.list(prefix, delimiter, start_after, token);
    };

    ObjectStorageParallelListingIterator iterator(
        "root/", /* num_threads */ 4, /* max_buffered_keys */ 256, list_level, list_level, descendAll);

    /// Ask for the first batch on a separate thread so the main thread can observe whether it returns before
    /// the (stalled) sub-directory listing is released.
    auto first_batch_future = std::async(std::launch::async, [&] { return iterator.getCurrentBatchAndScheduleNext(); });

    /// The first batch must become available without waiting for the release. A bounded wait that succeeds
    /// proves it was returned early; the old eager behavior would keep this pending until `release_sub`. The
    /// timeout only ever elapses if the regression is present, and it is generous enough to never trip on a
    /// loaded CI machine for an in-memory listing that normally completes in microseconds.
    const bool returned_early = first_batch_future.wait_for(std::chrono::seconds(30)) == std::future_status::ready;

    /// Always release the stalled listing so the worker (and the async task) can finish, regardless of the
    /// outcome, then join before asserting so a failure never leaks a blocked thread.
    release_sub.set_value();
    auto first_batch = first_batch_future.get();

    ASSERT_TRUE(returned_early) << "the current batch was withheld until the next (stalled) batch was produced";
    ASSERT_TRUE(first_batch.has_value());
    std::vector<std::string> first_paths;
    for (const auto & object : *first_batch)
        first_paths.push_back(object->relative_path);
    std::sort(first_paths.begin(), first_paths.end());
    EXPECT_EQ(first_paths, (std::vector<std::string>{"root/a.dat", "root/b.dat"}));

    /// The remainder (the sub-directory's key) is produced once released, and the whole listing completes.
    std::vector<std::string> rest;
    while (auto batch = iterator.getCurrentBatchAndScheduleNext())
        for (const auto & object : *batch)
            rest.push_back(object->relative_path);
    EXPECT_EQ(rest, (std::vector<std::string>{"root/sub/c.dat"}));
}
