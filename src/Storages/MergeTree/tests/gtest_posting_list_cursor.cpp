#include <gtest/gtest.h>
#include <Columns/ColumnVector.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>

#include <algorithm>
#include <numeric>
#include <random>
#include <set>
#include <vector>

using namespace DB;

namespace
{

/// A simple mock cursor for testing intersection/union algorithms without actual file I/O.
/// It wraps a sorted vector of row IDs and provides the same interface as PostingListCursor.
class MockPostingListCursor
{
public:
    explicit MockPostingListCursor(std::vector<uint32_t> row_ids_)
        : row_ids(std::move(row_ids_))
        , index(0)
        , is_valid(!row_ids.empty())
    {
        if (!row_ids.empty())
            density_val = static_cast<double>(row_ids.size()) / static_cast<double>(row_ids.back() - row_ids.front() + 1);
    }

    bool valid() const { return is_valid && index < row_ids.size(); }

    uint32_t value() const
    {
        if (!valid())
            throw std::runtime_error("MockPostingListCursor::value() called on invalid cursor");
        return row_ids[index];
    }

    void next()
    {
        if (valid())
            ++index;
        if (index >= row_ids.size())
            is_valid = false;
    }

    void seek(uint32_t target)
    {
        auto it = std::lower_bound(row_ids.begin() + static_cast<ptrdiff_t>(index), row_ids.end(), target);
        if (it == row_ids.end())
        {
            is_valid = false;
            return;
        }
        index = static_cast<size_t>(it - row_ids.begin());
    }

    /// For brute force algorithms
    void linearOr(UInt8 * data, size_t row_offset, size_t num_rows)
    {
        size_t row_end = row_offset + num_rows;
        for (uint32_t row_id : row_ids)
        {
            if (row_id >= row_offset && row_id < row_end)
                data[row_id - row_offset] = 1;
        }
    }

    void linearAnd(UInt8 * data, size_t row_offset, size_t num_rows)
    {
        size_t row_end = row_offset + num_rows;
        for (uint32_t row_id : row_ids)
        {
            if (row_id >= row_offset && row_id < row_end)
                ++data[row_id - row_offset];
        }
    }

private:
    std::vector<uint32_t> row_ids;
    size_t index = 0;
    bool is_valid = true;
    double density_val = 0.0;
};

using MockPostingListCursorPtr = std::shared_ptr<MockPostingListCursor>;

/// Compute set intersection using std::set for verification
std::set<uint32_t> computeIntersection(const std::vector<std::vector<uint32_t>> & posting_lists)
{
    if (posting_lists.empty())
        return {};

    std::set<uint32_t> result(posting_lists[0].begin(), posting_lists[0].end());

    for (size_t i = 1; i < posting_lists.size(); ++i)
    {
        std::set<uint32_t> current(posting_lists[i].begin(), posting_lists[i].end());
        std::set<uint32_t> intersection;
        std::set_intersection(
            result.begin(), result.end(),
            current.begin(), current.end(),
            std::inserter(intersection, intersection.begin()));
        result = std::move(intersection);
    }
    return result;
}

/// Compute set union using std::set for verification
std::set<uint32_t> computeUnion(const std::vector<std::vector<uint32_t>> & posting_lists)
{
    std::set<uint32_t> result;
    for (const auto & list : posting_lists)
    {
        result.insert(list.begin(), list.end());
    }
    return result;
}

/// Generate random sorted posting list
std::vector<uint32_t> generateRandomPostingList(size_t count, uint32_t max_row_id, uint32_t seed)
{
    std::mt19937 rng(seed);
    std::set<uint32_t> unique_ids;
    std::uniform_int_distribution<uint32_t> dist(0, max_row_id - 1);

    while (unique_ids.size() < count)
    {
        unique_ids.insert(dist(rng));
    }

    return std::vector<uint32_t>(unique_ids.begin(), unique_ids.end());
}

/// Helper to run brute force intersection and verify result
void verifyBruteForceIntersection(const std::vector<std::vector<uint32_t>> & posting_lists, size_t row_offset, size_t num_rows)
{
    if (posting_lists.empty())
        return;

    std::vector<MockPostingListCursorPtr> cursors;
    cursors.reserve(posting_lists.size());
    for (const auto & list : posting_lists)
        cursors.push_back(std::make_shared<MockPostingListCursor>(list));

    // Run brute force algorithm
    std::vector<UInt8> result(num_rows, 0);
    UInt8 * out = result.data();

    // First cursor does OR
    cursors[0]->linearOr(out, row_offset, num_rows);

    // Remaining cursors do AND (increment)
    for (size_t i = 1; i < cursors.size(); ++i)
        cursors[i]->linearAnd(out, row_offset, num_rows);

    // Finalize: check if count equals number of cursors
    size_t n = cursors.size();
    for (size_t i = 0; i < num_rows; ++i)
        out[i] = (out[i] == n);

    // Compute expected result
    auto expected = computeIntersection(posting_lists);

    // Verify
    for (size_t i = 0; i < num_rows; ++i)
    {
        uint32_t row_id = static_cast<uint32_t>(row_offset + i);
        bool in_result = (result[i] == 1);
        bool in_expected = (expected.count(row_id) > 0);
        EXPECT_EQ(in_result, in_expected)
            << "Mismatch at row_id=" << row_id << ": result=" << in_result << ", expected=" << in_expected;
    }
}

/// Helper to run union and verify result
void verifyUnion(const std::vector<std::vector<uint32_t>> & posting_lists, size_t row_offset, size_t num_rows)
{
    if (posting_lists.empty())
        return;

    std::vector<MockPostingListCursorPtr> cursors;
    cursors.reserve(posting_lists.size());
    for (const auto & list : posting_lists)
        cursors.push_back(std::make_shared<MockPostingListCursor>(list));

    // Run union algorithm (all cursors do OR)
    std::vector<UInt8> result(num_rows, 0);
    UInt8 * out = result.data();

    for (const auto & cursor : cursors)
        cursor->linearOr(out, row_offset, num_rows);

    // Compute expected result
    auto expected = computeUnion(posting_lists);

    // Verify
    for (size_t i = 0; i < num_rows; ++i)
    {
        uint32_t row_id = static_cast<uint32_t>(row_offset + i);
        bool in_result = (result[i] == 1);
        bool in_expected = (expected.count(row_id) > 0);
        EXPECT_EQ(in_result, in_expected)
            << "Mismatch at row_id=" << row_id << ": result=" << in_result << ", expected=" << in_expected;
    }
}

} // anonymous namespace

// =============================================================================
// MockPostingListCursor Basic Tests
// =============================================================================

TEST(PostingListCursorTest, MockCursorEmptyList)
{
    MockPostingListCursor cursor({});
    EXPECT_FALSE(cursor.valid());
}

TEST(PostingListCursorTest, MockCursorSingleElement)
{
    MockPostingListCursor cursor({42});
    EXPECT_TRUE(cursor.valid());
    EXPECT_EQ(cursor.value(), 42u);
    cursor.next();
    EXPECT_FALSE(cursor.valid());
}

TEST(PostingListCursorTest, MockCursorMultipleElements)
{
    MockPostingListCursor cursor({1, 5, 10, 20, 100});
    EXPECT_TRUE(cursor.valid());

    std::vector<uint32_t> values;
    while (cursor.valid())
    {
        values.push_back(cursor.value());
        cursor.next();
    }

    EXPECT_EQ(values, (std::vector<uint32_t>{1, 5, 10, 20, 100}));
}

TEST(PostingListCursorTest, SeekExactMatch)
{
    MockPostingListCursor cursor({1, 5, 10, 20, 100});
    cursor.seek(10);
    EXPECT_TRUE(cursor.valid());
    EXPECT_EQ(cursor.value(), 10u);
}

TEST(PostingListCursorTest, SeekBetweenElements)
{
    MockPostingListCursor cursor({1, 5, 10, 20, 100});
    cursor.seek(7);
    EXPECT_TRUE(cursor.valid());
    EXPECT_EQ(cursor.value(), 10u);
}

TEST(PostingListCursorTest, SeekPastEnd)
{
    MockPostingListCursor cursor({1, 5, 10, 20, 100});
    cursor.seek(200);
    EXPECT_FALSE(cursor.valid());
}

TEST(PostingListCursorTest, SeekToFirst)
{
    MockPostingListCursor cursor({1, 5, 10, 20, 100});
    cursor.seek(0);
    EXPECT_TRUE(cursor.valid());
    EXPECT_EQ(cursor.value(), 1u);
}

// =============================================================================
// LinearOr Tests
// =============================================================================

TEST(PostingListCursorTest, LinearOrEmptyList)
{
    MockPostingListCursor cursor({});
    std::vector<UInt8> result(100, 0);
    cursor.linearOr(result.data(), 0, 100);

    for (size_t i = 0; i < 100; ++i)
        EXPECT_EQ(result[i], 0u);
}

TEST(PostingListCursorTest, LinearOrSingleElement)
{
    MockPostingListCursor cursor({50});
    std::vector<UInt8> result(100, 0);
    cursor.linearOr(result.data(), 0, 100);

    EXPECT_EQ(result[50], 1u);
    size_t count = std::count(result.begin(), result.end(), 1);
    EXPECT_EQ(count, 1u);
}

TEST(PostingListCursorTest, LinearOrMultipleElements)
{
    MockPostingListCursor cursor({10, 20, 30, 40, 50});
    std::vector<UInt8> result(100, 0);
    cursor.linearOr(result.data(), 0, 100);

    EXPECT_EQ(result[10], 1u);
    EXPECT_EQ(result[20], 1u);
    EXPECT_EQ(result[30], 1u);
    EXPECT_EQ(result[40], 1u);
    EXPECT_EQ(result[50], 1u);

    size_t count = std::count(result.begin(), result.end(), 1);
    EXPECT_EQ(count, 5u);
}

TEST(PostingListCursorTest, LinearOrWithOffset)
{
    MockPostingListCursor cursor({100, 110, 120});
    std::vector<UInt8> result(50, 0);
    cursor.linearOr(result.data(), 100, 50);

    EXPECT_EQ(result[0], 1u);   // row_id 100 -> index 0
    EXPECT_EQ(result[10], 1u);  // row_id 110 -> index 10
    EXPECT_EQ(result[20], 1u);  // row_id 120 -> index 20

    size_t count = std::count(result.begin(), result.end(), 1);
    EXPECT_EQ(count, 3u);
}

TEST(PostingListCursorTest, LinearOrPartialOverlap)
{
    MockPostingListCursor cursor({10, 20, 30, 40, 50});
    std::vector<UInt8> result(25, 0);
    cursor.linearOr(result.data(), 15, 25);

    // Only row_ids 20, 30, 40 are in range [15, 40)
    EXPECT_EQ(result[5], 1u);   // row_id 20 -> index 5
    EXPECT_EQ(result[15], 1u);  // row_id 30 -> index 15
    // row_id 40 is at index 25, out of range

    size_t count = std::count(result.begin(), result.end(), 1);
    EXPECT_EQ(count, 2u);
}

// =============================================================================
// LinearAnd Tests
// =============================================================================

TEST(PostingListCursorTest, LinearAndSingleCursor)
{
    MockPostingListCursor cursor({10, 20, 30});
    std::vector<UInt8> result(50, 0);
    cursor.linearAnd(result.data(), 0, 50);

    EXPECT_EQ(result[10], 1u);
    EXPECT_EQ(result[20], 1u);
    EXPECT_EQ(result[30], 1u);
}

TEST(PostingListCursorTest, LinearAndMultipleCursorsIntersection)
{
    // List 1: {10, 20, 30, 40}
    // List 2: {15, 20, 30, 45}
    // Intersection: {20, 30}

    MockPostingListCursor cursor1({10, 20, 30, 40});
    MockPostingListCursor cursor2({15, 20, 30, 45});

    std::vector<UInt8> result(50, 0);

    // First cursor does OR
    cursor1.linearOr(result.data(), 0, 50);

    // Second cursor does AND (increment)
    cursor2.linearAnd(result.data(), 0, 50);

    // Finalize
    for (size_t i = 0; i < 50; ++i)
        result[i] = (result[i] == 2);

    EXPECT_EQ(result[20], 1u);
    EXPECT_EQ(result[30], 1u);

    size_t count = std::count(result.begin(), result.end(), 1);
    EXPECT_EQ(count, 2u);
}

// =============================================================================
// Brute Force Intersection Tests
// =============================================================================

TEST(PostingListCursorTest, IntersectionTwoListsNoOverlap)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3, 4, 5},
        {10, 11, 12, 13, 14}
    };
    verifyBruteForceIntersection(lists, 0, 20);
}

TEST(PostingListCursorTest, IntersectionTwoListsFullOverlap)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3, 4, 5},
        {1, 2, 3, 4, 5}
    };
    verifyBruteForceIntersection(lists, 0, 10);
}

TEST(PostingListCursorTest, IntersectionTwoListsPartialOverlap)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3, 4, 5, 6, 7},
        {4, 5, 6, 7, 8, 9, 10}
    };
    verifyBruteForceIntersection(lists, 0, 15);
}

TEST(PostingListCursorTest, IntersectionThreeLists)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        {2, 4, 6, 8, 10},
        {3, 6, 9}
    };
    // Intersection: {6}
    verifyBruteForceIntersection(lists, 0, 15);
}

TEST(PostingListCursorTest, IntersectionFourLists)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        {2, 4, 6, 8, 10},
        {4, 8},
        {4, 8, 12}
    };
    // Intersection: {4, 8}
    verifyBruteForceIntersection(lists, 0, 15);
}

TEST(PostingListCursorTest, IntersectionEmpty)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 3, 5, 7, 9},
        {2, 4, 6, 8, 10}
    };
    verifyBruteForceIntersection(lists, 0, 15);
}

TEST(PostingListCursorTest, IntersectionSingleList)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3, 4, 5}
    };
    verifyBruteForceIntersection(lists, 0, 10);
}

TEST(PostingListCursorTest, IntersectionWithOffset)
{
    std::vector<std::vector<uint32_t>> lists = {
        {100, 105, 110, 115, 120},
        {105, 110, 115, 125}
    };
    // Intersection in range [100, 130): {105, 110, 115}
    verifyBruteForceIntersection(lists, 100, 30);
}

TEST(PostingListCursorTest, IntersectionLargeRandomLists)
{
    std::vector<std::vector<uint32_t>> lists;
    for (uint32_t i = 0; i < 5; ++i)
    {
        lists.push_back(generateRandomPostingList(100, 1000, 42 + i));
    }
    verifyBruteForceIntersection(lists, 0, 1000);
}

// =============================================================================
// Union Tests
// =============================================================================

TEST(PostingListCursorTest, UnionTwoListsNoOverlap)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3},
        {10, 11, 12}
    };
    verifyUnion(lists, 0, 15);
}

TEST(PostingListCursorTest, UnionTwoListsFullOverlap)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3},
        {1, 2, 3}
    };
    verifyUnion(lists, 0, 10);
}

TEST(PostingListCursorTest, UnionTwoListsPartialOverlap)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 2, 3, 4, 5},
        {4, 5, 6, 7, 8}
    };
    verifyUnion(lists, 0, 15);
}

TEST(PostingListCursorTest, UnionThreeLists)
{
    std::vector<std::vector<uint32_t>> lists = {
        {1, 4, 7},
        {2, 5, 8},
        {3, 6, 9}
    };
    verifyUnion(lists, 0, 15);
}

TEST(PostingListCursorTest, UnionEmptyList)
{
    std::vector<std::vector<uint32_t>> lists = {
        {},
        {1, 2, 3}
    };
    verifyUnion(lists, 0, 10);
}

TEST(PostingListCursorTest, UnionWithOffset)
{
    std::vector<std::vector<uint32_t>> lists = {
        {100, 105, 110},
        {102, 108, 115}
    };
    verifyUnion(lists, 100, 20);
}

TEST(PostingListCursorTest, UnionLargeRandomLists)
{
    std::vector<std::vector<uint32_t>> lists;
    for (uint32_t i = 0; i < 5; ++i)
    {
        lists.push_back(generateRandomPostingList(100, 1000, 100 + i));
    }
    verifyUnion(lists, 0, 1000);
}

// =============================================================================
// Stress Tests
// =============================================================================

TEST(PostingListCursorTest, StressManySmallLists)
{
    std::vector<std::vector<uint32_t>> lists;
    for (uint32_t i = 0; i < 20; ++i)
    {
        lists.push_back(generateRandomPostingList(10, 100, 200 + i));
    }
    verifyBruteForceIntersection(lists, 0, 100);
    verifyUnion(lists, 0, 100);
}

TEST(PostingListCursorTest, StressFewLargeLists)
{
    std::vector<std::vector<uint32_t>> lists;
    for (uint32_t i = 0; i < 3; ++i)
    {
        lists.push_back(generateRandomPostingList(500, 2000, 300 + i));
    }
    verifyBruteForceIntersection(lists, 0, 2000);
    verifyUnion(lists, 0, 2000);
}

TEST(PostingListCursorTest, StressDensePostingLists)
{
    // Dense lists (high density)
    std::vector<uint32_t> list1(500);
    std::vector<uint32_t> list2(500);
    std::iota(list1.begin(), list1.end(), 0);      // 0..499
    std::iota(list2.begin(), list2.end(), 250);    // 250..749

    std::vector<std::vector<uint32_t>> lists = {list1, list2};
    verifyBruteForceIntersection(lists, 0, 1000);
    verifyUnion(lists, 0, 1000);
}

TEST(PostingListCursorTest, StressSparsePostingLists)
{
    // Sparse lists (low density)
    std::vector<uint32_t> list1, list2;
    for (uint32_t i = 0; i < 10000; i += 100)
        list1.push_back(i);
    for (uint32_t i = 50; i < 10000; i += 100)
        list2.push_back(i);

    std::vector<std::vector<uint32_t>> lists = {list1, list2};
    verifyBruteForceIntersection(lists, 0, 10000);
    verifyUnion(lists, 0, 10000);
}

TEST(PostingListCursorTest, StressMixedDensity)
{
    // Mix of dense and sparse lists
    std::vector<uint32_t> dense_list(200);
    std::iota(dense_list.begin(), dense_list.end(), 100);  // 100..299

    std::vector<uint32_t> sparse_list;
    for (uint32_t i = 0; i < 500; i += 10)
        sparse_list.push_back(i);

    std::vector<std::vector<uint32_t>> lists = {dense_list, sparse_list};
    verifyBruteForceIntersection(lists, 0, 500);
    verifyUnion(lists, 0, 500);
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST(PostingListCursorTest, EdgeCaseSingleElementLists)
{
    std::vector<std::vector<uint32_t>> lists = {
        {50},
        {50},
        {50}
    };
    verifyBruteForceIntersection(lists, 0, 100);
}

TEST(PostingListCursorTest, EdgeCaseSingleElementListsNoMatch)
{
    std::vector<std::vector<uint32_t>> lists = {
        {10},
        {20},
        {30}
    };
    verifyBruteForceIntersection(lists, 0, 100);
}

TEST(PostingListCursorTest, EdgeCaseConsecutiveElements)
{
    std::vector<std::vector<uint32_t>> lists = {
        {0, 1, 2, 3, 4},
        {2, 3, 4, 5, 6},
        {4, 5, 6, 7, 8}
    };
    // Intersection: {4}
    verifyBruteForceIntersection(lists, 0, 10);
}

TEST(PostingListCursorTest, EdgeCaseLargeGaps)
{
    std::vector<std::vector<uint32_t>> lists = {
        {0, 1000, 2000, 3000},
        {500, 1000, 1500, 2000, 2500, 3000},
        {1000, 2000, 3000, 4000}
    };
    // Intersection: {1000, 2000, 3000}
    verifyBruteForceIntersection(lists, 0, 5000);
}

TEST(PostingListCursorTest, EdgeCaseAllSameElement)
{
    std::vector<std::vector<uint32_t>> lists = {
        {42},
        {42},
        {42},
        {42},
        {42}
    };
    verifyBruteForceIntersection(lists, 0, 100);
}

