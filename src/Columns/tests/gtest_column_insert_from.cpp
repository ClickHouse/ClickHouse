#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>

#include <algorithm>
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
            auto dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeString(), std::move(keys));
            auto indexes = ColumnUInt16::create();
            for (UInt16 index : {UInt16(300), UInt16(1), UInt16(300)})
                indexes->insertValue(index);
            auto source_lc = ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), shared);
            const auto * source_lc_ptr = source_lc.get();
            auto destination_lc = ColumnLowCardinality::create(
                DataTypeLowCardinality::createColumnUnique(DataTypeString()), ColumnUInt8::create(), false);
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

TEST(ColumnInsertFrom, LowCardinalitySharedNullableDictionaryAndDetachment)
{
    DataTypeLowCardinality type(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    auto private_source = type.createColumn();
    private_source->insert(Field());
    private_source->insert(Field(String()));
    private_source->insert(Field(String("a\0b", 3)));
    const auto & source_lc = assert_cast<const ColumnLowCardinality &>(*private_source);
    auto source = ColumnLowCardinality::create(source_lc.getDictionaryPtr(), source_lc.getIndexes().getPtr(), true);
    private_source.reset();

    auto destination = type.createColumn();
    for (size_t row = 0; row < source->size(); ++row)
        destination->insertFrom(*source, row);

    const auto & destination_lc = assert_cast<const ColumnLowCardinality &>(*destination);
    ASSERT_EQ(destination_lc.getDictionaryPtr().get(), source->getDictionaryPtr().get());
    for (size_t row = 0; row < source->size(); ++row)
        EXPECT_EQ((*destination)[row], (*source)[row]);

    destination->insert(Field(String("new")));
    EXPECT_NE(destination_lc.getDictionaryPtr().get(), source->getDictionaryPtr().get());
    EXPECT_EQ((*destination)[3], Field(String("new")));
    EXPECT_EQ(source->size(), 3);
    EXPECT_TRUE(source->isNullAt(0));
    EXPECT_EQ((*source)[1], Field(String()));
    EXPECT_EQ((*source)[2], Field(String("a\0b", 3)));

    auto nonempty = type.createColumn();
    nonempty->insert(Field(String("sentinel")));
    nonempty->insertFrom(*source, 2);
    EXPECT_NE(assert_cast<const ColumnLowCardinality &>(*nonempty).getDictionaryPtr().get(), source->getDictionaryPtr().get());
    EXPECT_EQ((*nonempty)[0], Field(String("sentinel")));
    EXPECT_EQ((*nonempty)[1], (*source)[2]);
}
