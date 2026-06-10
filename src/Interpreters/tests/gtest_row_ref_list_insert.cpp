#include <Common/Arena.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/RowRefs.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(BuildRefList, InsertInitialElementFromEmpty)
{
    Arena pool;

    const UInt64 ref_word = BuildRef(/*block_no=*/7, /*row_no=*/42).word();

    /// Key point: default construction, empty list
    BuildRefList list;
    ASSERT_EQ(list.word, 0u);
    ASSERT_FALSE(list.begin().ok());

    /// Execute insert (hits the initialization branch)
    EXPECT_NO_THROW({
        list.insert(ref_word, pool);
    });

    /// The single element must be stored inline, with the singleton flag set
    EXPECT_TRUE(list.isSingleton());
    EXPECT_EQ(list.rows(), 1u);
    EXPECT_EQ(refWordBlockNo(list.word), 7u);
    EXPECT_EQ(refWordRowNo(list.word), 42u);

    /// Verify ForwardIterator behavior
    auto it = list.begin();
    ASSERT_TRUE(it.ok());
    EXPECT_EQ(*it, ref_word);

    ++it;
    EXPECT_FALSE(it.ok());
}

TEST(BuildRefList, InsertWithinOneNodeKeepsInsertionOrder)
{
    Arena pool;

    /// Up to MAX_LOCAL (7) rows live in the cell node only (head + slots), and one overflow node
    /// holds the rest; with a single overflow node iteration is exact insertion order.
    BuildRefList list(/*block_no=*/3, /*row_no=*/0);
    for (size_t row = 1; row < 10; ++row)
        list.insert(BuildRef(3, row).word(), pool);

    ASSERT_FALSE(list.isSingleton());
    EXPECT_EQ(list.rows(), 10u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
        order.push_back(refWordRowNo(*it));

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(order, expected);
}

TEST(BuildRefList, EvictionBoundaryKeepsInsertionOrder)
{
    Arena pool;

    /// Exactly at the chaining boundary (8 rows: head + 6 local + 1 evicted into the first
    /// overflow node) the order is still pure insertion order, and total_rows is correct.
    BuildRefList list(/*block_no=*/0, /*row_no=*/0);
    for (size_t row = 1; row < 8; ++row)
        list.insert(BuildRef(0, row).word(), pool);

    EXPECT_EQ(list.rows(), 8u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
        order.push_back(refWordRowNo(*it));

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 6, 7};
    EXPECT_EQ(order, expected);
}

TEST(BuildRefList, MultiNodeChainOrder)
{
    Arena pool;

    /// With two overflow nodes the order is head, local slots, then overflow nodes newest-first.
    BuildRefList list(/*block_no=*/2, /*row_no=*/0);
    for (size_t row = 1; row < 16; ++row)
        list.insert(BuildRef(2, row).word(), pool);

    EXPECT_EQ(list.rows(), 16u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        EXPECT_EQ(refWordBlockNo(*it), 2u);
        order.push_back(refWordRowNo(*it));
    }

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 6, 7, 8, 9, 10, 11};
    EXPECT_EQ(order, expected);
}

TEST(BuildRefList, CountSaturationStillIteratesEveryRow)
{
    Arena pool;

    /// Below the count sentinel rows() reads the word; at/after saturation it loads total_rows.
    /// Either way the iterator must yield every inserted row exactly once, in order.
    const size_t n = BuildRefList::COUNT_SAT + 5;
    BuildRefList list(/*block_no=*/0, /*row_no=*/0);

    EXPECT_EQ(list.rows(), 1u);
    for (size_t row = 1; row < BuildRefList::COUNT_SAT - 1; ++row)
        list.insert(BuildRef(0, row).word(), pool);
    /// Exact count straight from the word, no node load.
    EXPECT_EQ(list.rows(), BuildRefList::COUNT_SAT - 1);

    for (size_t row = BuildRefList::COUNT_SAT - 1; row < n; ++row)
        list.insert(BuildRef(0, row).word(), pool);
    /// Saturated: rows() now reflects total_rows loaded from the node.
    EXPECT_EQ(list.rows(), n);

    size_t count = 0;
    UInt64 seen_xor = 0;
    UInt64 expected_xor = 0;
    for (size_t row = 0; row < n; ++row)
        expected_xor ^= row;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        seen_xor ^= refWordRowNo(*it);
        ++count;
    }
    EXPECT_EQ(count, n);
    EXPECT_EQ(seen_xor, expected_xor);
}

TEST(BuildRefList, RangeRepresentation)
{
    Arena pool;

    BuildRefList list;
    list.setRange(BuildRef(/*block_no=*/1, /*row_no=*/100).word(), /*rows_=*/5, pool);

    ASSERT_FALSE(list.isSingleton());
    list.asBatch()->assertIsRange();
    EXPECT_EQ(list.rows(), 5u);

    std::vector<UInt32> rows;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        EXPECT_EQ(refWordBlockNo(*it), 1u);
        rows.push_back(refWordRowNo(*it));
    }

    const std::vector<UInt32> expected{100, 101, 102, 103, 104};
    EXPECT_EQ(rows, expected);
}
