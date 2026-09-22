#include <Common/Arena.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/RowRefs.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(RowRefList, InsertInitialElementFromEmpty)
{
    Arena pool;

    const UInt64 ref_word = RowRef(/*block_no=*/7, /*row_no=*/42).encode();

    /// Key point: default construction, empty list
    RowRefList list;
    ASSERT_EQ(list.word, 0u);
    ASSERT_FALSE(list.begin().ok());

    /// Execute insert (hits the initialization branch)
    EXPECT_NO_THROW({
        list.insert(ref_word, pool);
    });

    /// The single element must be stored inline, with the inline flag set
    EXPECT_TRUE(list.isInline());
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

TEST(RowRefList, InsertWithinOneNodeKeepsInsertionOrder)
{
    Arena pool;

    /// Up to MAX_LOCAL (7) rows live in the cell node only (head + slots), and one overflow node
    /// holds the rest; with a single overflow node iteration is exact insertion order.
    RowRefList list(/*block_no=*/3, /*row_no=*/0);
    for (size_t row = 1; row < 10; ++row)
        list.insert(RowRef(3, row).encode(), pool);

    ASSERT_FALSE(list.isInline());
    EXPECT_EQ(list.rows(), 10u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
        order.push_back(refWordRowNo(*it));

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(order, expected);
}

TEST(RowRefList, EvictionBoundaryKeepsInsertionOrder)
{
    Arena pool;

    /// Exactly at the chaining boundary (8 rows: head + 6 local + 1 evicted into the first
    /// overflow node) the order is still pure insertion order, and total_rows is correct.
    RowRefList list(/*block_no=*/0, /*row_no=*/0);
    for (size_t row = 1; row < 8; ++row)
        list.insert(RowRef(0, row).encode(), pool);

    EXPECT_EQ(list.rows(), 8u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
        order.push_back(refWordRowNo(*it));

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 6, 7};
    EXPECT_EQ(order, expected);
}

TEST(RowRefList, MultiNodeChainOrder)
{
    Arena pool;

    /// With two overflow nodes the order is head, local slots, then overflow nodes newest-first.
    RowRefList list(/*block_no=*/2, /*row_no=*/0);
    for (size_t row = 1; row < 16; ++row)
        list.insert(RowRef(2, row).encode(), pool);

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

TEST(RowRefList, CountSaturationStillIteratesEveryRow)
{
    Arena pool;

    /// Below the count sentinel rows() reads the word; at/after saturation it loads total_rows.
    /// Either way the iterator must yield every inserted row exactly once, in order.
    const size_t n = RowRefList::COUNT_SAT + 5;
    RowRefList list(/*block_no=*/0, /*row_no=*/0);

    EXPECT_EQ(list.rows(), 1u);
    for (size_t row = 1; row < RowRefList::COUNT_SAT - 1; ++row)
        list.insert(RowRef(0, row).encode(), pool);
    /// Exact count straight from the word, no node load.
    EXPECT_EQ(list.rows(), RowRefList::COUNT_SAT - 1);

    for (size_t row = RowRefList::COUNT_SAT - 1; row < n; ++row)
        list.insert(RowRef(0, row).encode(), pool);
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

TEST(RowRefList, RangeRepresentation)
{
    Arena pool;

    RowRefList list;
    list.setRange(RowRef(/*block_no=*/1, /*row_no=*/100).encode(), /*rows_=*/5, pool);

    ASSERT_FALSE(list.isInline());
    EXPECT_TRUE(list.asBatch()->is_range);
    EXPECT_EQ(list.rows(), 5u);

    std::vector<UInt32> rows;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        EXPECT_EQ(refWordBlockNo(*it), 1u);
        rows.push_back(refWordRowNo(*it));
    }

    const std::vector<UInt32> expected{100, 101, 102, 103, 104};
    EXPECT_EQ(rows, expected);

    /// A single-row range is stored as the inline ref itself: no node allocation.
    RowRefList single;
    single.setRange(RowRef(/*block_no=*/1, /*row_no=*/7).encode(), /*rows_=*/1, pool);
    EXPECT_TRUE(single.isInline());
    EXPECT_EQ(single.rows(), 1u);
    EXPECT_EQ(refWordRowNo(single.firstWord()), 7u);
}
