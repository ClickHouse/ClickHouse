#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

#include <algorithm>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

template <typename Create, typename MakeValue>
void checkSelfInsertionWithGrowth(Create create, MakeValue make_value)
{
    for (bool copy_last : {false, true})
    {
        auto column = create();
        auto & values = column->getData();
        using Value = std::remove_cvref_t<decltype(values[0])>;

        values.resize(1);
        values.resize(values.capacity());
        for (size_t i = 0; i < values.size(); ++i)
            values[i] = make_value(i);

        const std::vector<Value> before(values.begin(), values.end());
        const size_t source = copy_last ? before.size() - 1 : 0;
        column->insertFrom(*column, source);

        ASSERT_EQ(values.size(), before.size() + 1);
        ASSERT_GT(values.capacity(), before.size());
        EXPECT_EQ(values.back(), before[source]);
        EXPECT_TRUE(std::equal(before.begin(), before.end(), values.begin()));
    }
}

template <typename T>
void checkVectorSelfInsertion()
{
    checkSelfInsertionWithGrowth(
        [] { return ColumnVector<T>::create(); },
        [](size_t i) -> T { return static_cast<T>(std::numeric_limits<T>::max() - static_cast<T>(i)); });
}

template <typename T>
void checkDecimalSelfInsertion()
{
    checkSelfInsertionWithGrowth(
        [] { return ColumnDecimal<T>::create(0, 6); },
        [](size_t i) { return T{std::numeric_limits<typename T::NativeType>::max() - static_cast<typename T::NativeType>(i)}; });
}

}

TEST(ColumnInsertFrom, VectorSelfInsertionWithGrowth)
{
    checkVectorSelfInsertion<UInt8>();
    checkVectorSelfInsertion<UInt128>();
    checkVectorSelfInsertion<UInt256>();
}

TEST(ColumnInsertFrom, DecimalSelfInsertionWithGrowth)
{
    checkDecimalSelfInsertion<Decimal64>();
    checkDecimalSelfInsertion<Decimal128>();
    checkDecimalSelfInsertion<Decimal256>();
}

TEST(ColumnInsertFrom, NullableSelfInsertionWithNullMapGrowth)
{
    for (UInt8 is_null : {UInt8(0), UInt8(1)})
    {
        auto column = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
        auto & null_map = column->getNullMapData();
        null_map.resize(1);
        const size_t old_size = null_map.capacity();
        column->getNestedColumn().insertManyDefaults(old_size);
        null_map.resize(old_size);
        std::fill(null_map.begin(), null_map.end(), is_null);

        column->insertFrom(*column, 0);

        ASSERT_EQ(column->size(), old_size + 1);
        ASSERT_EQ(column->getNestedColumn().size(), old_size + 1);
        ASSERT_GT(null_map.capacity(), old_size);
        EXPECT_TRUE(std::all_of(null_map.begin(), null_map.end(), [is_null](UInt8 value) { return value == is_null; }));
    }
}

TEST(ColumnInsertFrom, ArraySelfInsertionWithGrowth)
{
    auto data = ColumnUInt64::create();
    auto & values = data->getData();
    values.resize(1);
    values.resize(values.capacity());
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (size_t i = 0; i < values.size(); ++i)
    {
        values[i] = i + 17;
        offsets->insertValue(i + 1);
    }
    const size_t old_size = values.size();
    auto column = ColumnArray::create(std::move(data), std::move(offsets));

    column->insertFrom(*column, old_size - 1);

    ASSERT_EQ(column->size(), old_size + 1);
    ASSERT_EQ(column->getData().size(), old_size + 1);
    for (size_t i = 0; i < old_size; ++i)
        EXPECT_EQ(column->getData().getUInt(i), i + 17);
    EXPECT_EQ(column->getData().getUInt(old_size), old_size + 16);
    for (size_t i = 0; i < column->size(); ++i)
        EXPECT_EQ(column->getOffsets()[i], i + 1);
}

TEST(ColumnInsertFrom, ArrayPreservesDynamicRangeStructure)
{
    auto data = ColumnDynamic::create(1);
    data->insert(Field(String("variant")));
    data->insert(Field(UInt64(1) << 40));
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (UInt64 offset : {UInt64(0), UInt64(1), UInt64(2)})
        offsets->insertValue(offset);
    auto source = ColumnArray::create(std::move(data), std::move(offsets));

    for (size_t row = 0; row < 3; ++row)
    {
        auto actual = ColumnArray::create(ColumnDynamic::create(1));
        auto expected = ColumnArray::create(ColumnDynamic::create(1));
        const size_t size = row == 0 ? 0 : 1;
        expected->getData().insertRangeFrom(source->getData(), row == 2 ? 1 : 0, size);
        expected->getOffsets().push_back(size);

        actual->insertFrom(*source, row);

        ASSERT_EQ(actual->size(), 1);
        EXPECT_EQ((*actual)[0], (*expected)[0]);
        EXPECT_TRUE(actual->dynamicStructureEquals(*expected));
    }
}
