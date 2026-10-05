#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>

#include <random>

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

namespace
{

/// Many short `insertRangeFrom` calls from sources with different dictionaries (as in vertical merges)
/// must produce the same column as inserting row by row with `insertFrom`: same values, same indexes
/// and the same dictionary order.
void testInsertRangeFromMatchesInsertFrom(bool nullable, bool shared_sources, size_t pool_size)
{
    DataTypePtr nested_type = std::make_shared<DataTypeString>();
    if (nullable)
        nested_type = std::make_shared<DataTypeNullable>(nested_type);
    auto type = std::make_shared<DataTypeLowCardinality>(nested_type);

    std::mt19937 rng(static_cast<unsigned>(pool_size * 4 + nullable * 2 + shared_sources));

    std::vector<Field> pool;
    for (size_t i = 0; i < pool_size; ++i)
    {
        if (nullable && i % 10 == 0)
            pool.emplace_back(Null());
        else
            pool.emplace_back(fmt::format("{}-{}", i, std::string(rng() % 36, 'x')));
    }

    std::vector<ColumnPtr> sources;
    for (size_t s = 0; s < 4; ++s)
    {
        auto source = type->createColumn();
        size_t num_distinct = 1 + rng() % pool_size;
        for (size_t row = 0; row < 1000; ++row)
        {
            /// Runs of equal values, like sorted key columns.
            size_t run = 1 + rng() % 8;
            const Field & value = pool[rng() % num_distinct];
            for (size_t i = 0; i < run; ++i)
                source->insert(value);
        }

        if (shared_sources)
        {
            const auto & lc = assert_cast<const ColumnLowCardinality &>(*source);
            sources.push_back(ColumnLowCardinality::create(lc.getDictionaryPtr(), lc.getIndexesPtr(), /* is_shared = */ true));
        }
        else
            sources.push_back(std::move(source));
    }

    auto result = type->createColumn();
    auto reference = type->createColumn();
    auto & reference_lc = assert_cast<ColumnLowCardinality &>(*reference);

    /// Lengths around the row-by-row threshold, zero, and long ranges that take the batch path.
    const std::vector<size_t> lengths = {0, 1, 2, 3, 7, 31, 32, 33, 63, 64, 65, 127, 128, 129, 500};
    for (size_t step = 0; step < 2000; ++step)
    {
        const auto & source = sources[rng() % sources.size()];
        const auto & source_lc = assert_cast<const ColumnLowCardinality &>(*source);
        size_t length = std::min(lengths[rng() % lengths.size()], source->size());
        size_t start = rng() % (source->size() - length + 1);

        result->insertRangeFrom(*source, start, length);

        /// Paths that do not insert keys into the dictionary are applied to the reference as is.
        bool dictionary_is_reused = length == 0 || &reference_lc.getDictionary() == &source_lc.getDictionary()
            || (reference->empty() && source_lc.isSharedDictionary());
        if (dictionary_is_reused)
            reference->insertRangeFrom(*source, start, length);
        else
            for (size_t row = start; row < start + length; ++row)
                reference->insertFrom(*source, row);
    }

    const auto & result_lc = assert_cast<const ColumnLowCardinality &>(*result);
    ASSERT_EQ(result->size(), reference->size());
    ASSERT_EQ(result_lc.getSizeOfIndexType(), reference_lc.getSizeOfIndexType());
    for (size_t row = 0; row < result->size(); ++row)
    {
        ASSERT_EQ((*result)[row], (*reference)[row]) << "row " << row;
        ASSERT_EQ(result_lc.getIndexes().getUInt(row), reference_lc.getIndexes().getUInt(row)) << "row " << row;
    }

    const auto & result_keys = *result_lc.getDictionary().getNestedColumn();
    const auto & reference_keys = *reference_lc.getDictionary().getNestedColumn();
    ASSERT_EQ(result_keys.size(), reference_keys.size());
    for (size_t i = 0; i < result_keys.size(); ++i)
        ASSERT_EQ(result_keys[i], reference_keys[i]) << "key " << i;
}

}

TEST(ColumnLowCardinality, InsertRangeFromMatchesInsertFrom)
{
    for (bool nullable : {false, true})
        for (bool shared_sources : {false, true})
            /// 1000 distinct keys widen the indexes from `UInt8` to `UInt16`.
            for (size_t pool_size : {1, 10, 1000})
                testInsertRangeFromMatchesInsertFrom(nullable, shared_sources, pool_size);
}
