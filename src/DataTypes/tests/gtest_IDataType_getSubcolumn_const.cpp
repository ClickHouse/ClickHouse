#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>

#include <gtest/gtest.h>

using namespace DB;

/// Regression test: IDataType::getSubcolumn and IDataType::tryGetSubcolumn must
/// unwrap a ColumnConst before extracting the subcolumn, and re-wrap the result
/// back into a ColumnConst of the original size.
///
/// The `COUNT(*)` fast path over `file(...)` with inferred `Nullable` columns
/// builds a ColumnConst(Nullable(...)) and asks for the `null` subcolumn. Before
/// the fix the ColumnConst reached the serialization-based extraction path and
/// threw an exception. This gtest exercises the branches directly so the fix
/// stays covered regardless of the end-to-end query being reproducible.
TEST(IDataTypeGetSubcolumn, UnwrapsColumnConst)
{
    auto type = makeNullable(std::make_shared<DataTypeString>());

    auto full_column = type->createColumn();
    full_column->insert("abc");
    ColumnPtr const_column = ColumnConst::create(std::move(full_column), 5);

    /// getSubcolumn: must succeed, return a ColumnConst of the same size,
    /// with the `null` subcolumn (UInt8 null map) inside.
    ColumnPtr got;
    EXPECT_NO_THROW(got = type->getSubcolumn("null", const_column));
    ASSERT_NE(got, nullptr);
    EXPECT_TRUE(isColumnConst(*got));
    EXPECT_EQ(got->size(), const_column->size());

    /// tryGetSubcolumn: same as above for an existing subcolumn.
    ColumnPtr tried;
    EXPECT_NO_THROW(tried = type->tryGetSubcolumn("null", const_column));
    ASSERT_NE(tried, nullptr);
    EXPECT_TRUE(isColumnConst(*tried));
    EXPECT_EQ(tried->size(), const_column->size());
}

/// tryGetSubcolumn must return nullptr (not throw) for a non-existent subcolumn
/// even when the column is a ColumnConst.
TEST(IDataTypeGetSubcolumn, TryGetSubcolumnMissingOverConst)
{
    auto type = makeNullable(std::make_shared<DataTypeString>());

    auto full_column = type->createColumn();
    full_column->insert("abc");
    ColumnPtr const_column = ColumnConst::create(std::move(full_column), 3);

    ColumnPtr tried;
    EXPECT_NO_THROW(tried = type->tryGetSubcolumn("this_subcolumn_does_not_exist", const_column));
    EXPECT_EQ(tried, nullptr);
}
