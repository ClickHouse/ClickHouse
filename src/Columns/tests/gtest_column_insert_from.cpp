#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <string>
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

TEST(ColumnInsertFrom, ArraySingletonPreservesSharedLowCardinalityDictionary)
{
    for (bool shared : {false, true})
    {
        for (bool tuple : {false, true})
        {
            auto keys = ColumnString::create();
            keys->insertDefault();
            for (size_t i = 1; i <= 300; ++i)
                keys->insert(Field(i == 300 ? String(8192, 'x') : std::to_string(i)));
            MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeString(), std::move(keys));
            auto indexes = ColumnUInt16::create();
            for (UInt16 index : {UInt16(300), UInt16(1), UInt16(300)})
                indexes->insertValue(index);
            MutableColumnPtr source_indexes = std::move(indexes);
            auto source_lc = ColumnLowCardinality::create(std::move(dictionary), std::move(source_indexes), shared);
            const auto * source_lc_ptr = source_lc.get();
            MutableColumnPtr destination_dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeString());
            MutableColumnPtr destination_indexes = ColumnUInt8::create();
            auto destination_lc = ColumnLowCardinality::create(
                std::move(destination_dictionary), std::move(destination_indexes), false);
            const auto * destination_lc_ptr = destination_lc.get();

            auto wrap = [tuple](MutableColumnPtr column) -> MutableColumnPtr
            {
                if (tuple)
                {
                    MutableColumns columns;
                    columns.push_back(std::move(column));
                    return ColumnTuple::create(std::move(columns));
                }
                return column;
            };

            auto offsets = ColumnArray::ColumnOffsets::create();
            for (UInt64 offset : {UInt64(1), UInt64(2), UInt64(3)})
                offsets->insertValue(offset);
            auto source = ColumnArray::create(wrap(std::move(source_lc)), std::move(offsets));
            auto destination = ColumnArray::create(wrap(std::move(destination_lc)));

            for (size_t row = 0; row < source->size(); ++row)
                destination->insertFrom(*source, row);

            ASSERT_EQ(destination->size(), source->size());
            for (size_t row = 0; row < source->size(); ++row)
                EXPECT_EQ((*destination)[row], (*source)[row]);
            EXPECT_EQ(destination_lc_ptr->isSharedDictionary(), shared);
            EXPECT_EQ(
                destination_lc_ptr->getDictionaryPtr().get() == source_lc_ptr->getDictionaryPtr().get(), shared);
            if (shared)
            {
                EXPECT_EQ(destination_lc_ptr->getIndexes().getUInt(0), 300);
                EXPECT_EQ(destination_lc_ptr->getIndexes().getUInt(1), 1);
                EXPECT_EQ(destination_lc_ptr->getIndexes().getUInt(2), 300);
            }
        }
    }
}

TEST(ColumnInsertFrom, ArrayLowCardinalityPreservesRangeSignedZero)
{
    auto dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeFloat64());
    const Float64 negative_zero = -0.0;
    const size_t source_index = dictionary->uniqueInsertData(reinterpret_cast<const char *>(&negative_zero), sizeof(negative_zero));
    ASSERT_GT(source_index, 0);
    ASSERT_LE(source_index, std::numeric_limits<UInt8>::max());
    MutableColumnPtr source_dictionary = std::move(dictionary);
    auto indexes = ColumnUInt8::create();
    indexes->insertValue(static_cast<UInt8>(source_index));
    MutableColumnPtr source_indexes = std::move(indexes);
    auto nested = ColumnLowCardinality::create(std::move(source_dictionary), std::move(source_indexes), false);
    ASSERT_EQ(std::bit_cast<UInt64>(nested->getFloat64(0)), UInt64(1) << 63);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insertValue(1);
    auto source = ColumnArray::create(std::move(nested), std::move(offsets));
    auto scalar = source->cloneEmpty();
    auto range = source->cloneEmpty();

    scalar->insertFrom(*source, 0);
    range->insertRangeFrom(*source, 0, 1);

    const auto & scalar_data = assert_cast<const ColumnArray &>(*scalar).getData();
    const auto & range_data = assert_cast<const ColumnArray &>(*range).getData();
    EXPECT_EQ(std::bit_cast<UInt64>(scalar_data.getFloat64(0)), std::bit_cast<UInt64>(range_data.getFloat64(0)));
    EXPECT_EQ(std::bit_cast<UInt64>(scalar_data.getFloat64(0)), UInt64(0));
}
