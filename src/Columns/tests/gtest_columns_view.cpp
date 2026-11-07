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

}
}
