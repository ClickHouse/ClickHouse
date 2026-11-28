#include <Columns/ColumnsNumber.h>
#include <Columns/groupColumns.h>
#include <gtest/gtest.h>

namespace DB
{
namespace
{

ColumnRawPtrs collect(std::span<const IColumn * const> columns)
{
    return ColumnRawPtrs(columns.begin(), columns.end());
}

TEST(GroupColumns, GroupsKeepOrderOfSourceColumns)
{
    /// Source columns of sizes 0, 1, 2, 3.
    Columns columns;
    ColumnRawPtrs column_ptrs;
    for (size_t size = 0; size != 4; ++size)
    {
        auto column = ColumnUInt64::create();
        for (size_t i = 0; i != size; ++i)
            column->insertValue(i);
        column_ptrs.push_back(column.get());
        columns.emplace_back(std::move(column));
    }

    /// Group 0: the columns of even size, group 1: the columns of odd size, group 2: all columns, group 3: no columns.
    auto groups = groupColumns(
        column_ptrs,
        4,
        [](const IColumn * column, auto && emit)
        {
            emit(column->size() % 2, column);
            emit(2, column);
        });
    ASSERT_EQ(groups.size(), 4u);
    EXPECT_EQ(collect(groups[0]), (ColumnRawPtrs{column_ptrs[0], column_ptrs[2]}));
    EXPECT_EQ(collect(groups[1]), (ColumnRawPtrs{column_ptrs[1], column_ptrs[3]}));
    EXPECT_EQ(collect(groups[2]), column_ptrs);
    EXPECT_TRUE(groups[3].empty());

    /// A source column may have no children.
    auto non_empty_groups = groupColumns(
        column_ptrs,
        2,
        [](const IColumn * column, auto && emit)
        {
            if (!column->empty())
                emit(column->size() % 2, column);
        });
    ASSERT_EQ(non_empty_groups.size(), 2u);
    EXPECT_EQ(collect(non_empty_groups[0]), (ColumnRawPtrs{column_ptrs[2]}));
    EXPECT_EQ(collect(non_empty_groups[1]), (ColumnRawPtrs{column_ptrs[1], column_ptrs[3]}));

    /// Without groups the children of the source columns are not visited.
    size_t visited = 0;
    auto no_groups = groupColumns(column_ptrs, 0, [&](const IColumn *, auto &&) { ++visited; });
    EXPECT_EQ(no_groups.size(), 0u);
    EXPECT_EQ(visited, 0u);
}

}
}
