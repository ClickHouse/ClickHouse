#include <DataTypes/DataTypeArray.h>
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

namespace
{

/// Array(Array(UInt64)) holding one row [[1, 2], [3]], wrapped in a ColumnConst of the given size.
std::pair<DataTypePtr, ColumnPtr> makeConstNestedArray(size_t const_size)
{
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()));
    auto full_column = type->createColumn();
    full_column->insert(Array{Array{UInt64(1), UInt64(2)}, Array{UInt64(3)}});
    return {type, ColumnConst::create(std::move(full_column), const_size)};
}

}

/// "sizeN" names the array-sizes subcolumn at nesting depth N counted from the column root, so the
/// same name denotes a differently shaped subcolumn at each level. Both resolving entry points take
/// the level the name was resolved at and must forward it through their ColumnConst recursion,
/// otherwise a wrapped column silently resets it to zero.
///
/// For Array(Array(UInt64)) the two readings of "size1" differ by type, which is what makes this
/// discriminating: at level 0 it is the inner sizes, Array(UInt64) = [2, 1]; at level 1 the leading
/// Array wrapper is already accounted for, so it is the outer size, UInt64 = 2.
TEST(IDataTypeGetSubcolumn, ForwardsInitialArrayLevelThroughColumnConst)
{
    constexpr size_t const_size = 5;
    auto [type, const_column] = makeConstNestedArray(const_size);

    auto level0 = type->getSubcolumn("size1", const_column, /*initial_array_level=*/0);
    ASSERT_NE(level0, nullptr);
    EXPECT_TRUE(isColumnConst(*level0));
    EXPECT_EQ(level0->size(), const_size);
    EXPECT_EQ(assert_cast<const ColumnConst &>(*level0).getDataColumnPtr()->getName(), "Array(UInt64)");
    EXPECT_EQ((*level0)[0], Field(Array{UInt64(2), UInt64(1)}));

    auto level1 = type->getSubcolumn("size1", const_column, /*initial_array_level=*/1);
    ASSERT_NE(level1, nullptr);
    EXPECT_TRUE(isColumnConst(*level1));
    EXPECT_EQ(level1->size(), const_size);
    EXPECT_EQ(assert_cast<const ColumnConst &>(*level1).getDataColumnPtr()->getName(), "UInt64");
    EXPECT_EQ((*level1)[0], Field(UInt64(2)));
}

/// The recursion is written out separately in tryGetSubcolumn, so it needs its own coverage.
TEST(IDataTypeGetSubcolumn, TryGetSubcolumnForwardsInitialArrayLevelThroughColumnConst)
{
    constexpr size_t const_size = 3;
    auto [type, const_column] = makeConstNestedArray(const_size);

    auto level0 = type->tryGetSubcolumn("size1", const_column, /*initial_array_level=*/0);
    ASSERT_NE(level0, nullptr);
    EXPECT_TRUE(isColumnConst(*level0));
    EXPECT_EQ(level0->size(), const_size);
    EXPECT_EQ(assert_cast<const ColumnConst &>(*level0).getDataColumnPtr()->getName(), "Array(UInt64)");
    EXPECT_EQ((*level0)[0], Field(Array{UInt64(2), UInt64(1)}));

    auto level1 = type->tryGetSubcolumn("size1", const_column, /*initial_array_level=*/1);
    ASSERT_NE(level1, nullptr);
    EXPECT_TRUE(isColumnConst(*level1));
    EXPECT_EQ(level1->size(), const_size);
    EXPECT_EQ(assert_cast<const ColumnConst &>(*level1).getDataColumnPtr()->getName(), "UInt64");
    EXPECT_EQ((*level1)[0], Field(UInt64(2)));
}
