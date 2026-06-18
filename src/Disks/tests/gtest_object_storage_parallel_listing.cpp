#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageParallelListingIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ParallelListingGlobPredicate.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>

#include <atomic>
#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <fmt/format.h>

using namespace DB;

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
    /// Requests that used a non-empty `StartAfter` and requests that used an empty `Delimiter` — both are
    /// unsupported by S3 Express / directory buckets, so the gated (no-keyspace-split) path must avoid them.
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

        const std::string & marker = token.empty() ? start_after : token;
        const bool marker_is_group = !delimiter.empty() && marker.ends_with(delimiter);

        ObjectStorageListResult res;
        size_t count = 0;
        std::string last_group;
        std::string last_item;

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
                        res.next_continuation_token = last_item;
                        return res;
                    }
                    res.common_prefixes.push_back(cp);
                    last_group = cp;
                    last_item = std::move(cp);
                    ++count;
                    continue;
                }
            }

            if (count >= page_size)
            {
                res.is_truncated = true;
                res.next_continuation_token = last_item;
                return res;
            }
            res.objects.push_back(std::make_shared<RelativePathWithMetadata>(key));
            last_item = key;
            ++count;
        }
        return res;
    }
};

ObjectStorageParallelListingIterator::ListLevelFunction makeListLevel(const FakeS3 & s3)
{
    return [&s3](const std::string & prefix, const std::string & delimiter, const std::string & start_after, const std::string & token)
    { return s3.list(prefix, delimiter, start_after, token); };
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

auto descendAll = [](const std::string &) { return true; };

/// Runs the iterator over `prefix` at several parallelism levels and asserts the produced key set is
/// exactly `expected` every time (complete, no duplicates), regardless of thread scheduling.
void assertCompleteForAllParallelism(const FakeS3 & s3, const std::string & prefix, const std::vector<std::string> & expected)
{
    for (size_t threads : {1, 2, 4, 16, 64})
    {
        ObjectStorageParallelListingIterator iterator(
            prefix, threads, /* max_buffered_keys */ 256, makeListLevel(s3), descendAll);
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
    const char * hex = "0123456789abcdef";
    /// Deterministic pseudo-random hex names (no Math.random available; derive from index).
    for (size_t i = 0; i < 6000; ++i)
    {
        std::string name = "mb/flat/";
        size_t x = i * 2654435761u + 12345u;
        for (size_t c = 0; c < 16; ++c)
        {
            name.push_back(hex[x & 0xf]);
            x = x * 1103515245u + 12345u;
        }
        name += ".txt.zst";
        s3.add(std::move(name));
    }
    s3.finalize();

    assertCompleteForAllParallelism(s3, "mb/flat/", expectedUnder(s3, "mb/flat/"));
}

TEST(ObjectStorageParallelListing, KeyspaceSplitCanBeDisabledForDirectoryBuckets)
{
    /// S3 Express / directory buckets reject `StartAfter` and only allow the '/' delimiter, so for them
    /// keyspace splitting is disabled. The same big flat directory must still be listed completely, but
    /// only via serial pagination — never issuing a `StartAfter` request or an empty delimiter — while the
    /// hierarchical delimiter walk (used here only at the root) stays available.
    auto fill = [](FakeS3 & s3)
    {
        const char * hex = "0123456789abcdef";
        for (size_t i = 0; i < 6000; ++i)
        {
            std::string name = "mb/flat/";
            size_t x = i * 2654435761u + 12345u;
            for (size_t c = 0; c < 16; ++c)
            {
                name.push_back(hex[x & 0xf]);
                x = x * 1103515245u + 12345u;
            }
            name += ".txt.zst";
            s3.add(std::move(name));
        }
        s3.finalize();
    };

    std::vector<std::string> expected;
    {
        FakeS3 s3;
        fill(s3);
        expected = expectedUnder(s3, "mb/flat/");
    }

    /// Sanity check the gate is meaningful: with splitting enabled, this uniform directory IS split, so
    /// `StartAfter` and empty-delimiter requests are indeed issued.
    {
        FakeS3 s3;
        s3.page_size = 50;
        fill(s3);
        ObjectStorageParallelListingIterator iterator(
            "mb/flat/", 16, /* max_buffered_keys */ 256, makeListLevel(s3), descendAll, /* allow_keyspace_split */ true);
        auto got = drain(iterator);
        std::sort(got.begin(), got.end());
        EXPECT_EQ(got, expected);
        EXPECT_GT(s3.requests_with_start_after.load(), 0u);
        EXPECT_GT(s3.requests_with_empty_delimiter.load(), 0u);
    }

    /// With splitting disabled, the listing is still complete, but no `StartAfter` and no empty delimiter
    /// are ever sent, regardless of the requested parallelism.
    for (size_t threads : {1, 4, 16})
    {
        FakeS3 s3;
        s3.page_size = 50;
        fill(s3);
        ObjectStorageParallelListingIterator iterator(
            "mb/flat/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), descendAll, /* allow_keyspace_split */ false);
        auto got = drain(iterator);
        std::sort(got.begin(), got.end());
        EXPECT_EQ(got, expected) << "threads=" << threads;
        EXPECT_EQ(s3.requests_with_start_after.load(), 0u) << "threads=" << threads;
        EXPECT_EQ(s3.requests_with_empty_delimiter.load(), 0u) << "threads=" << threads;
    }
}

TEST(ObjectStorageParallelListing, GapsOutsideSampledAlphabet)
{
    /// Mostly hex keys, plus a handful whose first byte after the prefix is OUTSIDE the hex alphabet
    /// (punctuation, uppercase, and a high byte). These bytes never appear on the hex pages, so a naive
    /// alphabet-bucket split would miss them; contiguous range-tiling must still cover them.
    FakeS3 s3;
    s3.page_size = 40;
    const char * hex = "0123456789abcdef";
    for (size_t i = 0; i < 3000; ++i)
    {
        std::string name = "p/";
        size_t x = i * 40503u + 7u;
        for (size_t c = 0; c < 12; ++c)
        {
            name.push_back(hex[x & 0xf]);
            x = x * 22695477u + 1u;
        }
        s3.add(std::move(name));
    }
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

    ObjectStorageParallelListingIterator iterator("s/", 8, 1000, makeListLevel(s3), descendAll);
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

TEST(ObjectStorageParallelListing, MixedHierarchicalAndFlat)
{
    /// A '/'-partitioned tree where each leaf directory is itself a big flat directory (needs both
    /// hierarchical descent and keyspace splitting).
    FakeS3 s3;
    s3.page_size = 25;
    const char * hex = "0123456789abcdef";
    for (int p = 0; p < 8; ++p)
        for (size_t i = 0; i < 400; ++i)
        {
            std::string name = "m/part=" + std::to_string(p) + "/";
            size_t x = (p * 1000 + i) * 2654435761u + 1u;
            for (size_t c = 0; c < 10; ++c)
            {
                name.push_back(hex[x & 0xf]);
                x = x * 1103515245u + 12345u;
            }
            s3.add(std::move(name));
        }
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
    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, makeListLevel(s3), should_descend);
    auto got = drain(iterator);
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, expectedUnder(s3, "root/keep/"));
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
            "root/", threads, /* max_buffered_keys */ 256, makeListLevel(s3), makeShouldDescendPredicate(glob));
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

    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, list_level, descendAll);
    EXPECT_THROW(drain(iterator), std::runtime_error);
}

TEST(ObjectStorageParallelListing, EmptyResult)
{
    FakeS3 s3;
    s3.finalize();
    ObjectStorageParallelListingIterator iterator("nothing/", 4, 1000, makeListLevel(s3), descendAll);
    EXPECT_TRUE(drain(iterator).empty());
}
