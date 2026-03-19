#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Columns/ColumnsNumber.h>

#include <algorithm>
#include <numeric>
#include <random>
#include <set>
#include <vector>

using namespace DB;

namespace
{

/// Build a `TokenPostingsInfo` with an embedded Roaring bitmap from sorted doc IDs.
TokenPostingsInfo makeEmbeddedInfo(const std::vector<uint32_t> & doc_ids)
{
    TokenPostingsInfo info;
    info.cardinality = static_cast<UInt32>(doc_ids.size());

    auto bitmap = std::make_shared<roaring::Roaring>();
    for (auto id : doc_ids)
        bitmap->add(id);
    info.embedded_postings = bitmap;

    if (!doc_ids.empty())
    {
        info.ranges.emplace_back(doc_ids.front(), doc_ids.back());
        info.offsets.emplace_back(); // dummy, not used for embedded
    }

    return info;
}

/// Construct an embedded cursor from a `TokenPostingsInfo`.
PostingListCursorPtr makeEmbeddedCursor(const TokenPostingsInfo & info)
{
    return std::make_shared<PostingListCursor>(info);
}

/// Generate an arithmetic sequence: {start, start+step, start+2*step, ...} of `count` elements.
std::vector<uint32_t> generateRange(uint32_t start, uint32_t count, uint32_t step = 1)
{
    std::vector<uint32_t> result;
    result.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        result.push_back(start + i * step);
    return result;
}

/// Drain all remaining doc IDs from a cursor via `next`.
std::vector<uint32_t> drainCursor(PostingListCursorPtr cursor)
{
    std::vector<uint32_t> result;
    while (cursor->valid())
    {
        result.push_back(cursor->value());
        cursor->next();
    }
    return result;
}

/// Perform `linearOr` into a buffer and return the positions that were set.
std::vector<uint32_t> linearOrToDocIds(PostingListCursorPtr cursor, size_t row_offset, size_t num_rows)
{
    std::vector<UInt8> buf(num_rows, 0);
    cursor->linearOr(buf.data(), row_offset, num_rows);
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (buf[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Perform intersection via `lazyIntersectPostingLists` and return matching doc IDs.
/// Use density_threshold=100.0 to force leapfrog, or density_threshold=0.0 to force brute-force.
std::vector<uint32_t> intersectAndCollect(
    PostingListCursorMap & postings,
    const std::vector<String> & tokens,
    size_t row_offset,
    size_t num_rows,
    float density_threshold = 100.0f)
{
    auto col = ColumnUInt8::create(num_rows, UInt8(0));
    lazyIntersectPostingLists(*col, postings, tokens, 0, row_offset, num_rows, density_threshold);
    const auto & data = col->getData();
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (data[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Perform union via `lazyUnionPostingLists` and return matching doc IDs.
std::vector<uint32_t> unionAndCollect(
    PostingListCursorMap & postings,
    const std::vector<String> & tokens,
    size_t row_offset,
    size_t num_rows)
{
    auto col = ColumnUInt8::create(num_rows, UInt8(0));
    lazyUnionPostingLists(*col, postings, tokens, 0, row_offset, num_rows);
    const auto & data = col->getData();
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (data[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

} // anonymous namespace


// =============================================================================
// Section 1: Basic Embedded Cursor
// =============================================================================

TEST(PostingListCursorTest, EmptyEmbeddedCursor)
{
    auto info = makeEmbeddedInfo({});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SingleDocEmbedded)
{
    auto info = makeEmbeddedInfo({42});
    auto cursor = makeEmbeddedCursor(info);

    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 42u);
    EXPECT_EQ(cursor->cardinality(), 1u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, MultipleDocsSequentialIteration)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, LargeEmbeddedCursor)
{
    auto docs = generateRange(0, 6);
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, SparseEmbeddedCursor)
{
    auto docs = generateRange(100, 5, 100); // 100, 200, 300, 400, 500
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, NextAfterInvalidIsNoop)
{
    auto info = makeEmbeddedInfo({5});
    auto cursor = makeEmbeddedCursor(info);
    cursor->next();
    EXPECT_FALSE(cursor->valid());
    cursor->next(); // should not cause exception
    EXPECT_FALSE(cursor->valid());
    cursor->next(); // multiple calls after invalid
    EXPECT_FALSE(cursor->valid());
}


// =============================================================================
// Section 2: Seek Operations
// =============================================================================

TEST(PostingListCursorTest, SeekToExactValue)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(30);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);
}

TEST(PostingListCursorTest, SeekToNonExistentGoesToNext)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(25);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);
}

TEST(PostingListCursorTest, SeekBeyondLastInvalidates)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(31);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SeekToFirstDoc)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(10);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 10u);
}

TEST(PostingListCursorTest, SeekToLastDoc)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(30);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SeekToZero)
{
    std::vector<uint32_t> docs = {0, 5, 10};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(0);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 0u);
}

TEST(PostingListCursorTest, SeekBeforeFirst)
{
    std::vector<uint32_t> docs = {100, 200, 300};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(50);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 100u);
}

TEST(PostingListCursorTest, SeekProgressivelyForward)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50, 60};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(25);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);

    cursor->seek(55);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 60u);

    cursor->seek(61);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SeekThenNext)
{
    std::vector<uint32_t> docs = {5, 10, 15, 20, 25, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(15);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 15u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 20u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 25u);
}


// =============================================================================
// Section 3: Density and Cardinality
// =============================================================================

TEST(PostingListCursorTest, CardinalityReflectsDocCount)
{
    auto info = makeEmbeddedInfo({1, 2, 3, 4, 5});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_EQ(cursor->cardinality(), 5u);
}

TEST(PostingListCursorTest, DensityPerfectlyDense)
{
    auto docs = generateRange(0, 5); // 0, 1, 2, 3, 4
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}

TEST(PostingListCursorTest, DensitySparse)
{
    std::vector<uint32_t> docs = {0, 100};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_NEAR(cursor->density(), 2.0 / 101.0, 1e-6);
}

TEST(PostingListCursorTest, DensitySingleDoc)
{
    auto info = makeEmbeddedInfo({42});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}


// =============================================================================
// Section 4: linearOr
// =============================================================================

TEST(PostingListCursorTest, LinearOrFullRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 60);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, LinearOrPartialRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 15, 30); // rows [15, 45)
    std::vector<uint32_t> expected = {20, 30, 40};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, LinearOrNoOverlap)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 100, 50);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, LinearOrSingleRowMatch)
{
    std::vector<uint32_t> docs = {42};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 42, 1);
    EXPECT_EQ(result, std::vector<uint32_t>{42});
}

TEST(PostingListCursorTest, LinearOrDenseRange)
{
    auto docs = generateRange(100, 6); // 100..105
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 110);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 5: linearAnd
// =============================================================================

TEST(PostingListCursorTest, LinearAndFullRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(60, 0);
    cursor->linearAnd(buf.data(), 0, 60);

    for (auto d : docs)
        EXPECT_EQ(buf[d], 1u) << "Expected buf[" << d << "] == 1";
    EXPECT_EQ(buf[0], 0u);
    EXPECT_EQ(buf[15], 0u);
}

TEST(PostingListCursorTest, LinearAndIncrementsExisting)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(40, 1);
    cursor->linearAnd(buf.data(), 0, 40);

    EXPECT_EQ(buf[10], 2u);
    EXPECT_EQ(buf[20], 2u);
    EXPECT_EQ(buf[30], 2u);
    EXPECT_EQ(buf[0], 1u);
    EXPECT_EQ(buf[5], 1u);
}


// =============================================================================
// Section 6: Two-cursor Intersection (leapfrog)
// =============================================================================

TEST(PostingListCursorTest, IntersectTwoIdentical)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 60, 100.0f);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectTwoDisjoint)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({15, 25, 35});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 50, 100.0f);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, IntersectTwoPartialOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50});
    auto info2 = makeEmbeddedInfo({20, 30, 60, 70});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 80, 100.0f);
    std::vector<uint32_t> expected = {20, 30};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectTwoSingleCommon)
{
    auto info1 = makeEmbeddedInfo({1, 50, 100});
    auto info2 = makeEmbeddedInfo({49, 50, 51});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 150, 100.0f);
    EXPECT_EQ(result, std::vector<uint32_t>{50});
}


// =============================================================================
// Section 7: Three-cursor Intersection
// =============================================================================

TEST(PostingListCursorTest, IntersectThreeAllMatch)
{
    auto docs = generateRange(0, 5, 10); // 0, 10, 20, 30, 40
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);
    auto info3 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 50, 100.0f);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectThreePartialOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50, 60});
    auto info2 = makeEmbeddedInfo({15, 20, 30, 45, 60});
    auto info3 = makeEmbeddedInfo({20, 25, 30, 55, 60});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 70, 100.0f);
    std::vector<uint32_t> expected = {20, 30, 60};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectThreeNoCommon)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({11, 21, 31});
    auto info3 = makeEmbeddedInfo({12, 22, 32});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 50, 100.0f);
    EXPECT_TRUE(result.empty());
}


// =============================================================================
// Section 8: Four-cursor Intersection
// =============================================================================

TEST(PostingListCursorTest, IntersectFourAllOverlap)
{
    auto docs = generateRange(0, 5, 10); // 0, 10, 20, 30, 40
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);
    auto info3 = makeEmbeddedInfo(docs);
    auto info4 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);
    postings["d"] = makeEmbeddedCursor(info4);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d"}, 0, 50, 100.0f);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectFourMixedSelectivity)
{
    // LCM(2, 3, 5, 7) = 210
    auto docs1 = generateRange(0, 50, 2);  // every 2nd: 0, 2, 4, ...
    auto docs2 = generateRange(0, 34, 3);  // every 3rd: 0, 3, 6, ...
    auto docs3 = generateRange(0, 20, 5);  // every 5th: 0, 5, 10, ...
    auto docs4 = generateRange(0, 15, 7);  // every 7th: 0, 7, 14, ...

    auto info1 = makeEmbeddedInfo(docs1);
    auto info2 = makeEmbeddedInfo(docs2);
    auto info3 = makeEmbeddedInfo(docs3);
    auto info4 = makeEmbeddedInfo(docs4);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);
    postings["d"] = makeEmbeddedCursor(info4);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d"}, 0, 220, 100.0f);

    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i < 220; i += 210)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 9: Five+ Cursors (linear / heap leapfrog)
// =============================================================================

TEST(PostingListCursorTest, IntersectFiveCursors)
{
    // Primes: 2, 3, 5, 7, 11 -> LCM = 2310
    auto docs1 = generateRange(0, 50, 2);
    auto docs2 = generateRange(0, 34, 3);
    auto docs3 = generateRange(0, 20, 5);
    auto docs4 = generateRange(0, 15, 7);
    auto docs5 = generateRange(0, 10, 11);

    auto info1 = makeEmbeddedInfo(docs1);
    auto info2 = makeEmbeddedInfo(docs2);
    auto info3 = makeEmbeddedInfo(docs3);
    auto info4 = makeEmbeddedInfo(docs4);
    auto info5 = makeEmbeddedInfo(docs5);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);
    postings["d"] = makeEmbeddedCursor(info4);
    postings["e"] = makeEmbeddedCursor(info5);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d", "e"}, 0, 120, 100.0f);
    // Only 0 is common (LCM = 2310, next would be 2310 which is >= 120)
    EXPECT_EQ(result, std::vector<uint32_t>{0});
}

TEST(PostingListCursorTest, IntersectNineCursorsHeap)
{
    // Nine identical cursors — triggers heap leapfrog (n > 8)
    auto docs = generateRange(0, 3, 20); // 0, 20, 40
    std::vector<TokenPostingsInfo> infos(9);
    PostingListCursorMap postings;
    std::vector<String> tokens;

    for (int i = 0; i < 9; ++i)
    {
        infos[i] = makeEmbeddedInfo(docs);
        String name = "t" + std::to_string(i);
        tokens.push_back(name);
    }
    for (int i = 0; i < 9; ++i)
        postings[tokens[i]] = makeEmbeddedCursor(infos[i]);

    auto result = intersectAndCollect(postings, tokens, 0, 50, 100.0f);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 10: Brute-force Intersection
// =============================================================================

TEST(PostingListCursorTest, BruteForceIntersectionDense)
{
    // Dense postings with low density threshold force brute-force path
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50});
    auto info2 = makeEmbeddedInfo({20, 30, 60});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // density_threshold = 0.0 forces brute-force path (min_density >= 0)
    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 70, 0.0f);
    std::vector<uint32_t> expected = {20, 30};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, BruteForceVsLeapfrogConsistency)
{
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(0, 99);

    for (int trial = 0; trial < 10; ++trial)
    {
        std::vector<TokenPostingsInfo> infos_bf(3);
        std::vector<TokenPostingsInfo> infos_lf(3);
        std::vector<std::vector<uint32_t>> all_docs(3);

        for (int i = 0; i < 3; ++i)
        {
            std::set<uint32_t> s;
            size_t count = 10 + trial * 5;
            while (s.size() < count)
                s.insert(dist(rng));
            all_docs[i].assign(s.begin(), s.end());
            infos_bf[i] = makeEmbeddedInfo(all_docs[i]);
            infos_lf[i] = makeEmbeddedInfo(all_docs[i]);
        }

        // Brute force (density_threshold = 0.0)
        PostingListCursorMap postings_bf;
        postings_bf["a"] = makeEmbeddedCursor(infos_bf[0]);
        postings_bf["b"] = makeEmbeddedCursor(infos_bf[1]);
        postings_bf["c"] = makeEmbeddedCursor(infos_bf[2]);
        auto bf_result = intersectAndCollect(postings_bf, {"a", "b", "c"}, 0, 100, 0.0f);

        // Leapfrog (density_threshold = 100.0 to force leapfrog)
        PostingListCursorMap postings_lf;
        postings_lf["a"] = makeEmbeddedCursor(infos_lf[0]);
        postings_lf["b"] = makeEmbeddedCursor(infos_lf[1]);
        postings_lf["c"] = makeEmbeddedCursor(infos_lf[2]);
        auto lf_result = intersectAndCollect(postings_lf, {"a", "b", "c"}, 0, 100, 100.0f);

        EXPECT_EQ(bf_result, lf_result) << "Brute-force vs leapfrog mismatch at trial " << trial;
    }
}


// =============================================================================
// Section 11: Union Operations
// =============================================================================

TEST(PostingListCursorTest, UnionTwoOverlapping)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40});
    auto info2 = makeEmbeddedInfo({20, 30, 50});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 60);
    std::vector<uint32_t> expected = {10, 20, 30, 40, 50};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, UnionThreeDisjoint)
{
    auto info1 = makeEmbeddedInfo({1, 4, 7});
    auto info2 = makeEmbeddedInfo({2, 5, 8});
    auto info3 = makeEmbeddedInfo({3, 6, 9});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = unionAndCollect(postings, {"a", "b", "c"}, 0, 10);
    std::vector<uint32_t> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, UnionSingleCursor)
{
    std::vector<uint32_t> docs = {5, 15, 25};
    auto info = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info);

    auto result = unionAndCollect(postings, {"a"}, 0, 30);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, UnionEmptyMap)
{
    PostingListCursorMap postings;
    auto result = unionAndCollect(postings, {}, 0, 100);
    EXPECT_TRUE(result.empty());
}


// =============================================================================
// Section 12: Edge Cases
// =============================================================================

TEST(PostingListCursorTest, SeekOnInvalidCursorIsNoop)
{
    auto info = makeEmbeddedInfo({10, 20});
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(30); // beyond range, invalidates
    EXPECT_FALSE(cursor->valid());

    cursor->seek(10); // seek on invalid cursor
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, LinearOrOnEmbeddedWithOffset)
{
    std::vector<uint32_t> docs = {100, 200, 300, 400, 500};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    // Window [200, 400) with row_offset=200, num_rows=200
    auto result = linearOrToDocIds(cursor, 200, 200);
    std::vector<uint32_t> expected = {200, 300};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectWithMissingToken)
{
    auto info = makeEmbeddedInfo({1, 2, 3});

    PostingListCursorMap postings;
    postings["exists"] = makeEmbeddedCursor(info);

    // "missing" token is not in map. Only 1 cursor found, n=1 path triggers `linearOr`.
    auto result = intersectAndCollect(postings, {"exists", "missing"}, 0, 10, 100.0f);
    std::vector<uint32_t> expected = {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectWithRowOffset)
{
    auto info1 = makeEmbeddedInfo({100, 200, 300, 400, 500});
    auto info2 = makeEmbeddedInfo({100, 200, 300, 400, 500});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // Only look at rows [200, 400)
    auto result = intersectAndCollect(postings, {"a", "b"}, 200, 200, 100.0f);
    std::vector<uint32_t> expected = {200, 300};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectZeroCursors)
{
    PostingListCursorMap postings;
    auto result = intersectAndCollect(postings, {}, 0, 100, 100.0f);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, IntersectSingleCursor)
{
    auto info = makeEmbeddedInfo({5, 10, 15, 20, 25});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info);

    auto result = intersectAndCollect(postings, {"a"}, 0, 30, 100.0f);
    std::vector<uint32_t> expected = {5, 10, 15, 20, 25};
    EXPECT_EQ(result, expected);
}
