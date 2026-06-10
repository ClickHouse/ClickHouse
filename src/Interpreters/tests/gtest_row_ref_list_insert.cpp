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

TEST(BuildRefList, InsertDuplicatesPreservesLegacyOrder)
{
    Arena pool;

    /// Insert rows 0..9 of block 3 under one key and check the iteration order matches
    /// the order of the old RowRefList: head, then the newest batch, then older batches
    /// (each batch in insertion order).
    BuildRefList list(/*block_no=*/3, /*row_no=*/0);
    for (size_t row = 1; row < 10; ++row)
        list.insert(BuildRef(3, row).word(), pool);

    ASSERT_FALSE(list.isSingleton());
    EXPECT_EQ(list.rows(), 10u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
        order.push_back(refWordRowNo(*it));

    /// Batch::MAX_SIZE is 7: rows 1..7 fill the first batch, rows 8..9 go to the newer batch
    /// that is iterated first.
    const std::vector<UInt32> expected{0, 8, 9, 1, 2, 3, 4, 5, 6, 7};
    EXPECT_EQ(order, expected);
}

TEST(BuildRefList, RangeRepresentation)
{
    Arena pool;

    BuildRefList list;
    list.setRange(BuildRef(/*block_no=*/1, /*row_no=*/100).word(), /*rows_=*/5, pool);

    ASSERT_FALSE(list.isSingleton());
    list.asBlob()->assertIsRange();
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
