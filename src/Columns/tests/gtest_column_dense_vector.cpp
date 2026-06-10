#include <Columns/ColumnDenseVector.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeVector.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadBufferFromString.h>
#include <Common/Arena.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

MutableColumnPtr createVectorColumn(size_t dimension, const std::vector<std::vector<Float32>> & rows)
{
    auto column = DataTypeVector(std::make_shared<DataTypeFloat32>(), dimension).createColumn();
    for (const auto & row : rows)
    {
        Array array;
        for (const auto value : row)
            array.push_back(Field(Float64(value)));
        column->insert(array);
    }
    return column;
}

Field makeRow(const std::vector<Float32> & row)
{
    Array array;
    for (const auto value : row)
        array.push_back(Field(Float64(value)));
    return array;
}

}

TEST(ColumnDenseVector, CreateAndInsertField)
{
    auto column = createVectorColumn(3, {{1, 2, 3}, {4, 5, 6}});
    ASSERT_EQ(column->size(), 2);

    const auto & vec = assert_cast<const ColumnDenseVector &>(*column);
    ASSERT_EQ(vec.getDimension(), 3);
    ASSERT_EQ(vec.getElementType(), TypeIndex::Float32);
    ASSERT_EQ(vec.getValueSize(), 3 * sizeof(Float32));
    ASSERT_EQ(vec.getNestedColumn().size(), 6);

    ASSERT_EQ((*column)[0], makeRow({1, 2, 3}));
    ASSERT_EQ((*column)[1], makeRow({4, 5, 6}));

    /// Inserting an array of the wrong size must throw, not pad.
    ASSERT_ANY_THROW(column->insert(makeRow({1, 2})));
    ASSERT_FALSE(column->tryInsert(makeRow({1, 2, 3, 4})));
    ASSERT_EQ(column->size(), 2);
}

TEST(ColumnDenseVector, InsertFromAndRange)
{
    auto src = createVectorColumn(2, {{1, 2}, {3, 4}, {5, 6}});
    auto dst = createVectorColumn(2, {});

    dst->insertFrom(*src, 1);
    dst->insertRangeFrom(*src, 0, 3);
    dst->insertManyFrom(*src, 2, 2);
    dst->insertDefault();

    ASSERT_EQ(dst->size(), 7);
    ASSERT_EQ((*dst)[0], makeRow({3, 4}));
    ASSERT_EQ((*dst)[1], makeRow({1, 2}));
    ASSERT_EQ((*dst)[3], makeRow({5, 6}));
    ASSERT_EQ((*dst)[4], makeRow({5, 6}));
    ASSERT_EQ((*dst)[5], makeRow({5, 6}));
    ASSERT_EQ((*dst)[6], makeRow({0, 0}));
    ASSERT_TRUE(dst->isDefaultAt(6));
    ASSERT_FALSE(dst->isDefaultAt(0));

    dst->popBack(5);
    ASSERT_EQ(dst->size(), 2);
    ASSERT_EQ((*dst)[1], makeRow({1, 2}));
}

TEST(ColumnDenseVector, FilterPermuteIndexReplicate)
{
    auto column = createVectorColumn(2, {{1, 1}, {2, 2}, {3, 3}, {4, 4}});

    IColumn::Filter filter{1, 0, 0, 1};
    auto filtered = column->filter(filter, -1);
    ASSERT_EQ(filtered->size(), 2);
    ASSERT_EQ((*filtered)[0], makeRow({1, 1}));
    ASSERT_EQ((*filtered)[1], makeRow({4, 4}));

    IColumn::Permutation permutation{3, 2, 1, 0};
    auto permuted = column->permute(permutation, 0);
    ASSERT_EQ(permuted->size(), 4);
    ASSERT_EQ((*permuted)[0], makeRow({4, 4}));
    ASSERT_EQ((*permuted)[3], makeRow({1, 1}));

    auto indexes = ColumnUInt8::create();
    indexes->insertValue(2);
    indexes->insertValue(0);
    auto indexed = column->index(*indexes, 0);
    ASSERT_EQ(indexed->size(), 2);
    ASSERT_EQ((*indexed)[0], makeRow({3, 3}));
    ASSERT_EQ((*indexed)[1], makeRow({1, 1}));

    IColumn::Offsets offsets{0, 2, 3, 5};
    auto replicated = column->replicate(offsets);
    ASSERT_EQ(replicated->size(), 5);
    ASSERT_EQ((*replicated)[0], makeRow({2, 2}));
    ASSERT_EQ((*replicated)[1], makeRow({2, 2}));
    ASSERT_EQ((*replicated)[2], makeRow({3, 3}));
    ASSERT_EQ((*replicated)[3], makeRow({4, 4}));
    ASSERT_EQ((*replicated)[4], makeRow({4, 4}));
}

TEST(ColumnDenseVector, CompareAndPermutation)
{
    auto column = createVectorColumn(2, {{2, 1}, {1, 2}, {1, 10}, {1, 2}});

    ASSERT_EQ(column->compareAt(1, 3, *column, 1), 0);
    ASSERT_LT(column->compareAt(1, 0, *column, 1), 0);
    ASSERT_GT(column->compareAt(2, 1, *column, 1), 0);

    IColumn::Permutation permutation;
    column->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Stable, 0, 1, permutation);
    ASSERT_EQ((*column)[permutation[0]], makeRow({1, 2}));
    ASSERT_EQ((*column)[permutation[1]], makeRow({1, 2}));
    ASSERT_EQ((*column)[permutation[2]], makeRow({1, 10}));
    ASSERT_EQ((*column)[permutation[3]], makeRow({2, 1}));
}

TEST(ColumnDenseVector, ArenaRoundTrip)
{
    auto src = createVectorColumn(3, {{1, 2, 3}, {4, 5, 6}});
    auto dst = createVectorColumn(3, {});

    Arena arena;
    const char * begin = nullptr;
    auto first = src->serializeValueIntoArena(0, arena, begin, nullptr);
    begin = nullptr;
    auto second = src->serializeValueIntoArena(1, arena, begin, nullptr);
    ASSERT_EQ(first.size(), 3 * sizeof(Float32));

    ReadBufferFromString first_buf({first.data(), first.size()});
    dst->deserializeAndInsertFromArena(first_buf, nullptr);
    ReadBufferFromString second_buf({second.data(), second.size()});
    dst->deserializeAndInsertFromArena(second_buf, nullptr);

    ASSERT_EQ(dst->size(), 2);
    ASSERT_EQ((*dst)[0], makeRow({1, 2, 3}));
    ASSERT_EQ((*dst)[1], makeRow({4, 5, 6}));
}

TEST(ColumnDenseVector, FixedSizeProperties)
{
    auto column = createVectorColumn(4, {{1, 2, 3, 4}});
    ASSERT_TRUE(column->valuesHaveFixedSize());
    /// Must stay false: true would route the column into ColumnFixedSizeHelper-based key packing.
    ASSERT_FALSE(column->isFixedAndContiguous());
    ASSERT_EQ(column->sizeOfValueIfFixed(), 4 * sizeof(Float32));
    ASSERT_EQ(column->getRawData().size(), 4 * sizeof(Float32));
    ASSERT_EQ(column->byteSizeAt(0), 4 * sizeof(Float32));
}
