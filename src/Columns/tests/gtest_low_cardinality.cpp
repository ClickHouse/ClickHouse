#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>

#include <bit>
#include <cmath>

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

TEST(ColumnLowCardinality, HasOnlyTypeDefaults)
{
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto column = low_cardinality_type->createColumn();

    ASSERT_TRUE(column->hasOnlyTypeDefaults());
    column->insertDefault();
    column->insert(Field{UInt64{0}});
    ASSERT_TRUE(column->hasOnlyTypeDefaults());

    column->insert(Field{UInt64{1}});
    ASSERT_FALSE(column->hasOnlyTypeDefaults());
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

TEST(ColumnLowCardinality, CloneNullableKeepsZeroValue)
{
    auto data_type = std::make_shared<DataTypeUInt64>();
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();

    column->insert(static_cast<UInt64>(0));
    column->insert(static_cast<UInt64>(1));
    column->insert(static_cast<UInt64>(2));

    auto nullable_column = assert_cast<const ColumnLowCardinality &>(*column).cloneNullable();
    const auto & nullable_lc = assert_cast<const ColumnLowCardinality &>(*nullable_column);

    ASSERT_TRUE(nullable_lc.nestedIsNullable());
    ASSERT_FALSE(nullable_lc.isNullAt(0));
    ASSERT_FALSE(nullable_lc.isNullAt(1));
    ASSERT_FALSE(nullable_lc.isNullAt(2));

    Field value;
    nullable_column->get(0, value);
    ASSERT_EQ(value.safeGet<UInt64>(), 0);
    nullable_column->get(1, value);
    ASSERT_EQ(value.safeGet<UInt64>(), 1);
    nullable_column->get(2, value);
    ASSERT_EQ(value.safeGet<UInt64>(), 2);
}

TEST(ColumnLowCardinality, InsertRangeFromChecksBoundsAfterSharingDictionary)
{
    auto dictionary_keys = ColumnUInt64::create();
    for (UInt64 value : {0, 10})
        dictionary_keys->insertValue(value);

    ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeUInt64(), std::move(dictionary_keys));

    auto source_indexes = ColumnUInt8::create();
    source_indexes->insertValue(1);
    auto source = ColumnLowCardinality::create(dictionary, std::move(source_indexes), /* is_shared = */ true);

    auto wide_indexes = ColumnUInt16::create();
    wide_indexes->insertValue(1);
    auto wide_column = ColumnLowCardinality::create(dictionary, std::move(wide_indexes), /* is_shared = */ false);
    auto destination = wide_column->cloneEmpty();
    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);

    ASSERT_EQ(low_cardinality_destination.getSizeOfIndexType(), sizeof(UInt16));
    EXPECT_THROW(destination->insertRangeFrom(*source, source->size(), 1), Exception);
    EXPECT_TRUE(destination->empty());
}

static void testEmptyDestinationPreservesFloatingPointCanonicalization(MutableColumnPtr keys, const DataTypePtr & nested_type)
{
    SCOPED_TRACE(nested_type->getName());
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, std::move(keys));
    auto indexes = ColumnUInt8::create();
    indexes->getData().assign({1, 2, 3});
    auto source = ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/true);
    auto destination = std::make_shared<DataTypeLowCardinality>(nested_type)->createColumn();

    destination->insertRangeFrom(*source, 0, source->size());

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    ASSERT_EQ(low_cardinality_destination.getDictionary().size(), 2);
    ASSERT_EQ(destination->size(), 3);
    EXPECT_EQ(low_cardinality_destination.getIndexes().getUInt(0), 0);
    EXPECT_EQ(low_cardinality_destination.getIndexes().getUInt(1), 1);
    EXPECT_EQ(low_cardinality_destination.getIndexes().getUInt(2), 1);
    EXPECT_FALSE(std::signbit(destination->getFloat64(0)));
    EXPECT_TRUE(std::isnan(destination->getFloat64(1)));
    EXPECT_TRUE(std::isnan(destination->getFloat64(2)));
}

TEST(ColumnLowCardinality, EmptyDestinationPreservesFloatingPointCanonicalization)
{
    auto float64_keys = ColumnFloat64::create();
    auto & float64_key_data = float64_keys->getData();
    float64_key_data.push_back(0.0);
    float64_key_data.push_back(-0.0);
    float64_key_data.push_back(std::bit_cast<Float64>(UInt64{0x7ff8000000000001ULL}));
    float64_key_data.push_back(std::bit_cast<Float64>(UInt64{0x7ff8000000000002ULL}));
    testEmptyDestinationPreservesFloatingPointCanonicalization(
        std::move(float64_keys), std::make_shared<DataTypeFloat64>());

    auto bfloat16_keys = ColumnBFloat16::create();
    auto & bfloat16_key_data = bfloat16_keys->getData();
    bfloat16_key_data.push_back(BFloat16::fromBits(0x0000));
    bfloat16_key_data.push_back(BFloat16::fromBits(0x8000));
    bfloat16_key_data.push_back(BFloat16::fromBits(0x7fc1));
    bfloat16_key_data.push_back(BFloat16::fromBits(0x7fc2));
    testEmptyDestinationPreservesFloatingPointCanonicalization(
        std::move(bfloat16_keys), std::make_shared<DataTypeBFloat16>());
}

static void testSharedFloatingPointDictionaryUsesValueComparisons(const ColumnPtr & dictionary)
{
    SCOPED_TRACE(dictionary->getName());
    auto indexes = ColumnUInt8::create();
    indexes->getData().assign({2, 0, 3, 1});
    auto column = ColumnLowCardinality::create(dictionary, std::move(indexes), /*is_shared=*/true);

    auto reversed_zero_indexes = ColumnUInt8::create();
    reversed_zero_indexes->getData().assign({1, 0});
    auto reversed_zero_column = ColumnLowCardinality::create(dictionary, std::move(reversed_zero_indexes), /*is_shared=*/true);

    IColumn::Permutation stable_permutation;
    reversed_zero_column->getPermutation(
        IColumn::PermutationSortDirection::Ascending,
        IColumn::PermutationSortStability::Stable,
        0,
        1,
        stable_permutation);

    ASSERT_EQ(stable_permutation.size(), 2);
    EXPECT_EQ(stable_permutation[0], 0);
    EXPECT_EQ(stable_permutation[1], 1);

    IColumn::Permutation permutation{0, 1, 2, 3};
    EqualRanges equal_ranges{{0, 4}};
    column->updatePermutation(
        IColumn::PermutationSortDirection::Ascending,
        IColumn::PermutationSortStability::Stable,
        0,
        1,
        permutation,
        equal_ranges);

    ASSERT_EQ(permutation.size(), 4);
    EXPECT_EQ(permutation[0], 1);
    EXPECT_EQ(permutation[1], 3);
    EXPECT_EQ(permutation[2], 0);
    EXPECT_EQ(permutation[3], 2);
    ASSERT_EQ(equal_ranges.size(), 2);
    EXPECT_EQ(equal_ranges[0].from, 0);
    EXPECT_EQ(equal_ranges[0].to, 2);
    EXPECT_EQ(equal_ranges[1].from, 2);
    EXPECT_EQ(equal_ranges[1].to, 4);

    auto tiebreaker = ColumnUInt64::create();
    tiebreaker->getData().assign({40, 30, 20, 10});
    tiebreaker->updatePermutation(
        IColumn::PermutationSortDirection::Ascending,
        IColumn::PermutationSortStability::Stable,
        0,
        1,
        permutation,
        equal_ranges);

    ASSERT_EQ(permutation.size(), 4);
    EXPECT_EQ(permutation[0], 3);
    EXPECT_EQ(permutation[1], 1);
    EXPECT_EQ(permutation[2], 2);
    EXPECT_EQ(permutation[3], 0);

    auto equal_indexes = ColumnUInt8::create();
    equal_indexes->getData().assign({0, 1});
    auto equal_column = ColumnLowCardinality::create(dictionary, std::move(equal_indexes), /*is_shared=*/true);
    EXPECT_FALSE(equal_column->hasEqualValues());

    auto equal_nan_indexes = ColumnUInt8::create();
    equal_nan_indexes->getData().assign({2, 3});
    auto equal_nan_column = ColumnLowCardinality::create(dictionary, std::move(equal_nan_indexes), /*is_shared=*/true);
    EXPECT_FALSE(equal_nan_column->hasEqualValues());

    IColumn::Permutation identity_permutation{0, 1, 2, 3};
    EXPECT_EQ(column->estimateCardinalityInPermutedRange(identity_permutation, {0, 4}), 2);
}

TEST(ColumnLowCardinality, SharedFloatingPointDictionaryUsesValueComparisons)
{
    auto float64_keys = ColumnFloat64::create();
    auto & float64_key_data = float64_keys->getData();
    float64_key_data.push_back(0.0);
    float64_key_data.push_back(-0.0);
    float64_key_data.push_back(std::bit_cast<Float64>(UInt64{0x7ff8000000000001ULL}));
    float64_key_data.push_back(std::bit_cast<Float64>(UInt64{0x7ff8000000000002ULL}));
    testSharedFloatingPointDictionaryUsesValueComparisons(
        DataTypeLowCardinality::createColumnUnique(DataTypeFloat64(), std::move(float64_keys)));

    auto bfloat16_keys = ColumnBFloat16::create();
    auto & bfloat16_key_data = bfloat16_keys->getData();
    bfloat16_key_data.push_back(BFloat16::fromBits(0x0000));
    bfloat16_key_data.push_back(BFloat16::fromBits(0x8000));
    bfloat16_key_data.push_back(BFloat16::fromBits(0x7fc1));
    bfloat16_key_data.push_back(BFloat16::fromBits(0x7fc2));
    testSharedFloatingPointDictionaryUsesValueComparisons(
        DataTypeLowCardinality::createColumnUnique(DataTypeBFloat16(), std::move(bfloat16_keys)));
}

TEST(ColumnLowCardinality, SharedNullableFloatingPointDictionaryCardinality)
{
    auto keys = ColumnFloat64::create();
    auto & key_data = keys->getData();
    key_data.push_back(0.0);
    key_data.push_back(0.0);
    key_data.push_back(-0.0);
    key_data.push_back(std::bit_cast<Float64>(UInt64{0x7ff8000000000001ULL}));
    key_data.push_back(std::bit_cast<Float64>(UInt64{0x7ff8000000000002ULL}));

    auto nested_type = makeNullable(std::make_shared<DataTypeFloat64>());
    ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, std::move(keys));
    auto indexes = ColumnUInt8::create();
    indexes->getData().assign({0, 1, 2, 3, 4});
    auto column = ColumnLowCardinality::create(dictionary, std::move(indexes), /*is_shared=*/true);

    IColumn::Permutation identity_permutation{0, 1, 2, 3, 4};
    EXPECT_EQ(column->estimateCardinalityInPermutedRange(identity_permutation, {0, 5}), 3);
}

TEST(ColumnLowCardinality, EmptyDictionaryEmptyIndexes)
{
    /// Test edge case: empty dictionary (size=0) with empty indexes (num_rows=0)
    /// This should not throw an error, as empty indexes are always valid
    /// Regression test for bug where check was: if (max_position >= limit)
    /// When num_rows=0, max_position stays 0, and with limit=0, this incorrectly threw

    auto data_type = std::make_shared<DataTypeUInt32>();
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();
    auto & lc_column = assert_cast<ColumnLowCardinality &>(*column);

    // Create empty keys and indexes columns
    auto empty_keys = ColumnUInt32::create();
    auto empty_indexes = ColumnUInt8::create();

    // This should NOT throw an exception
    ASSERT_NO_THROW(lc_column.insertRangeFromDictionaryEncodedColumn(*empty_keys, *empty_indexes));

    ASSERT_EQ(column->size(), 0);
}
