#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsView.h>
#include <gtest/gtest.h>

namespace DB
{
namespace
{

const IColumn * keepNonEmpty(const IColumn * column, const void *)
{
    return column->empty() ? nullptr : column;
}

const IColumn * keepSelected(const IColumn * column, const void * context)
{
    return column == static_cast<const IColumn *>(context) ? column : nullptr;
}

const IColumn * filterAll(const IColumn *, const void *)
{
    return nullptr;
}

ColumnRawPtrs collect(const ColumnsView & columns)
{
    ColumnRawPtrs result;
    columns.forEach([&](const IColumn * column) { result.push_back(column); });
    return result;
}

TEST(ColumnsView, FilteredViewIntrospection)
{
    auto empty_column = ColumnUInt64::create();
    auto first_column = ColumnUInt64::create();
    first_column->insertValue(1);
    auto second_column = ColumnUInt64::create();
    second_column->insertValue(2);

    const IColumn * first_column_ptr = first_column.get();
    Columns columns;
    columns.emplace_back(std::move(empty_column));
    columns.emplace_back(std::move(first_column));
    columns.emplace_back(std::move(second_column));

    ColumnsView source_columns(columns);

    auto non_empty_columns = source_columns.filterProject(keepNonEmpty);
    EXPECT_FALSE(non_empty_columns.empty());
    EXPECT_EQ(non_empty_columns.size(), 2u);
    EXPECT_FALSE(non_empty_columns.tryGetSingle().has_value());

    auto selected_column = source_columns.filterProject(keepSelected, first_column_ptr);
    EXPECT_FALSE(selected_column.empty());
    EXPECT_EQ(selected_column.size(), 1u);
    ASSERT_TRUE(selected_column.tryGetSingle().has_value());
    EXPECT_EQ(*selected_column.tryGetSingle(), first_column_ptr);

    auto empty_view = source_columns.filterProject(filterAll);
    EXPECT_TRUE(empty_view.empty());
    EXPECT_EQ(empty_view.size(), 0u);
    EXPECT_FALSE(empty_view.tryGetSingle().has_value());
}

TEST(ColumnsView, GroupColumns)
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

    ColumnsView source_columns(columns);

    /// Group 0: the columns of even size, group 1: the columns of odd size, group 2: all columns, group 3: no columns.
    auto groups = groupColumns(
        source_columns,
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
    EXPECT_EQ(groups[2].size(), 4u);
    EXPECT_FALSE(groups[2].tryGetSingle().has_value());
    EXPECT_TRUE(groups[3].empty());
    EXPECT_EQ(groups[3].size(), 0u);
    EXPECT_FALSE(groups[3].tryGetSingle().has_value());

    /// The source columns can be any view, for example a filtered one.
    auto non_empty_groups = groupColumns(
        source_columns.filterProject(keepNonEmpty), 2, [](const IColumn * column, auto && emit) { emit(column->size() % 2, column); });
    ASSERT_EQ(non_empty_groups.size(), 2u);
    ASSERT_TRUE(non_empty_groups[0].tryGetSingle().has_value());
    EXPECT_EQ(*non_empty_groups[0].tryGetSingle(), column_ptrs[2]);
    EXPECT_EQ(collect(non_empty_groups[1]), (ColumnRawPtrs{column_ptrs[1], column_ptrs[3]}));

    /// Without groups the children of the source columns are not visited.
    size_t visited = 0;
    auto no_groups = groupColumns(source_columns, 0, [&](const IColumn *, auto &&) { ++visited; });
    EXPECT_EQ(no_groups.size(), 0u);
    EXPECT_EQ(visited, 0u);
}
}
}
