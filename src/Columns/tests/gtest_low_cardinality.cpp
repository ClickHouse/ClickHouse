#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnUniqueCompressed.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <gtest/gtest.h>

#include <vector>

using namespace DB;

template <typename T>
void testLowCardinalityNumberInsert(const DataTypePtr & data_type)
{
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();

    column->insert(static_cast<T>(15));
    column->insert(static_cast<T>(20));
    column->insert(static_cast<T>(25));

    Field value;
    column->get(0, value);
    ASSERT_EQ(value.safeGet<T>(), 15);

    column->get(1, value);
    ASSERT_EQ(value.safeGet<T>(), 20);

    column->get(2, value);
    ASSERT_EQ(value.safeGet<T>(), 25);
}

TEST(ColumnLowCardinality, Insert)
{
    testLowCardinalityNumberInsert<UInt8>(std::make_shared<DataTypeUInt8>());
    testLowCardinalityNumberInsert<UInt16>(std::make_shared<DataTypeUInt16>());
    testLowCardinalityNumberInsert<UInt32>(std::make_shared<DataTypeUInt32>());
    testLowCardinalityNumberInsert<UInt64>(std::make_shared<DataTypeUInt64>());
    testLowCardinalityNumberInsert<UInt128>(std::make_shared<DataTypeUInt128>());
    testLowCardinalityNumberInsert<UInt256>(std::make_shared<DataTypeUInt256>());

    testLowCardinalityNumberInsert<Int8>(std::make_shared<DataTypeInt8>());
    testLowCardinalityNumberInsert<Int16>(std::make_shared<DataTypeInt16>());
    testLowCardinalityNumberInsert<Int32>(std::make_shared<DataTypeInt32>());
    testLowCardinalityNumberInsert<Int64>(std::make_shared<DataTypeInt64>());
    testLowCardinalityNumberInsert<Int128>(std::make_shared<DataTypeInt128>());
    testLowCardinalityNumberInsert<Int256>(std::make_shared<DataTypeInt256>());

    testLowCardinalityNumberInsert<BFloat16>(std::make_shared<DataTypeBFloat16>());
    testLowCardinalityNumberInsert<Float32>(std::make_shared<DataTypeFloat32>());
    testLowCardinalityNumberInsert<Float64>(std::make_shared<DataTypeFloat64>());
}

TEST(ColumnLowCardinality, Clone)
{
    auto data_type = std::make_shared<DataTypeInt32>();
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();
    ASSERT_FALSE(assert_cast<const ColumnLowCardinality &>(*column).nestedIsNullable());

    auto nullable_column = assert_cast<const ColumnLowCardinality &>(*column).cloneNullable();

    ASSERT_TRUE(assert_cast<const ColumnLowCardinality &>(*nullable_column).nestedIsNullable());
    ASSERT_FALSE(assert_cast<const ColumnLowCardinality &>(*column).nestedIsNullable());
}

TEST(ColumnLowCardinality, CompressedDictionary)
{
    MutableColumnPtr indexes = ColumnUInt8::create();
    MutableColumnPtr dictionary = ColumnUniqueFCBlockDF::create(ColumnString::create(), 4, false);
    auto low_cardinality_column = ColumnLowCardinality::create(std::move(dictionary), std::move(indexes));


    std::vector<String> data = {
        "banana",
        "banner",
        "apple",
        "application",
        "banana",
        "banana"
    };

    for (const auto & str : data)
    {
        low_cardinality_column->insert(str);
    }

    for (size_t i = 0; i < data.size(); ++i)
    {
        EXPECT_EQ((*low_cardinality_column)[i].safeGet<String>(), data[i]);
    }
}

TEST(ColumnLowCardinality, CompressedDictionaryCompact)
{
    std::vector<String> data = {
        "banana",
        "banner",
        "application",
        "apple",
        "fly",
        "flying",
        "fascinating",
        "fantasy",
        "arrow",
        "amazing",
    };

    MutableColumnPtr indexes = ColumnUInt8::create();
    MutableColumnPtr dictionary = ColumnUniqueFCBlockDF::create(ColumnString::create(), 4, false);
    auto low_cardinality_column = ColumnLowCardinality::create(std::move(dictionary), std::move(indexes));

    for (const auto & str : data)
    {
        low_cardinality_column->insert(str);
    }

    auto cut_column = low_cardinality_column->cutAndCompact(2, 6);

    EXPECT_EQ(cut_column->size(), 6);
    for (size_t i = 0; i < cut_column->size(); ++i)
    {
        EXPECT_EQ((*cut_column)[i].safeGet<String>(), data[i + 2]);
    }
}

TEST(ColumnLowCardinality, CompressedDictionaryNullable)
{
    MutableColumnPtr indexes = ColumnUInt8::create();
    MutableColumnPtr dictionary = ColumnUniqueFCBlockDF::create(ColumnString::create(), 4, true);
    auto low_cardinality_column = ColumnLowCardinality::create(std::move(dictionary), std::move(indexes));

    std::vector<String> data = {
        "banana",
        "banner",
        "apple",
        "application",
    };

    for (const auto & str : data)
    {
        low_cardinality_column->insert(str);
    }

    for (size_t i = 0; i < data.size(); ++i)
    {
        EXPECT_FALSE(low_cardinality_column->isNullAt(i));
        EXPECT_EQ((*low_cardinality_column)[i].safeGet<String>(), data[i]);
    }

    low_cardinality_column->insert({});
    const size_t null_pos = low_cardinality_column->size() - 1;
    EXPECT_TRUE(low_cardinality_column->isNullAt(null_pos));
}
