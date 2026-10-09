#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

namespace
{

/// A sparse column of `size` default `UInt64` values.
ColumnPtr makeSparseUInt64(size_t size)
{
    auto values = ColumnUInt64::create();
    values->insertDefault(); /// The value at position 0 is the default one.
    return ColumnSparse::create(std::move(values), ColumnUInt64::create(), size);
}

Block makeTupleBlock(ColumnPtr nested, DataTypePtr nested_type)
{
    auto type = std::make_shared<DataTypeTuple>(DataTypes{std::move(nested_type)});
    return Block{ColumnWithTypeAndName{ColumnTuple::create(Columns{std::move(nested)}), std::move(type), "t"}};
}

}

/// A `Sparse` column is interchangeable with the full column it wraps at any nesting depth,
/// not only at the top level: one branch of a query can materialize a nested subcolumn while
/// another one keeps it sparse.
GTEST_TEST(BlockStructure, NestedSparseIsCompatibleWithFull)
{
    const size_t size = 3;
    auto uint64_type = std::make_shared<DataTypeUInt64>();

    Block sparse = makeTupleBlock(makeSparseUInt64(size), uint64_type);
    Block full = makeTupleBlock(ColumnUInt64::create(size, 0), uint64_type);

    EXPECT_TRUE(blocksHaveEqualStructure(sparse, full));
    EXPECT_TRUE(blocksHaveEqualStructure(full, sparse));
    EXPECT_TRUE(blocksHaveEqualStructure(sparse, sparse));
}

/// The nested comparison stays strict for everything else: unwrapping `Sparse` must not make
/// structurally different nested columns compare equal.
GTEST_TEST(BlockStructure, NestedSparseIsNotCompatibleWithAnotherColumn)
{
    const size_t size = 3;
    auto uint64_type = std::make_shared<DataTypeUInt64>();

    Block sparse = makeTupleBlock(makeSparseUInt64(size), uint64_type);
    /// The declared type is the same, only the column inside the tuple is a different one.
    Block other = makeTupleBlock(ColumnString::create(), uint64_type);

    EXPECT_FALSE(blocksHaveEqualStructure(sparse, other));
    EXPECT_FALSE(blocksHaveEqualStructure(other, sparse));
}
