#include <Common/Arena.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/RowRefs.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(RowRefList, InsertInitialElementFromEmpty)
{
    Arena pool;

    Columns columns;
    columns.emplace_back(ColumnInt32::create());
    ColumnsInfo columns_info(std::move(columns));

    RowRef row_ref(&columns_info, /*row_num=*/42);

    /// Key point: default construction, rows == 0
    RowRefList list;
    ASSERT_EQ(list.rows, 0u);

    /// Execute insert (hits the newly added initialization branch)
    EXPECT_NO_THROW({
        list.insert(RowRef(row_ref), pool);
    });

    /// Verify rows is properly initialized
    EXPECT_EQ(list.rows, 1u);

    /// Root element must be initialized
    EXPECT_EQ(list.columns_info, &columns_info);
    EXPECT_EQ(list.row_num, 42u);

    /// No Batch should be created
    /// When rows == 1, next must remain nullptr
    list.assertIsRange();

    /// Verify ForwardIterator behavior
    auto it = list.begin();
    ASSERT_TRUE(it.ok());

    const RowRef * ref = *it;
    ASSERT_NE(ref, nullptr);
    EXPECT_EQ(ref->columns_info, &columns_info);
    EXPECT_EQ(ref->row_num, 42u);

    ++it;
    EXPECT_FALSE(it.ok());
}
