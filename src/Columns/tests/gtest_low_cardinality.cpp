#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>

#include <bit>
#include <functional>
#include <limits>
#include <pcg_random.hpp>

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

MutableColumnPtr makeLowCardinality(const DataTypePtr & type, const std::vector<Field> & values)
{
    auto column = std::make_shared<DataTypeLowCardinality>(type)->createColumn();
    for (const auto & value : values)
        column->insert(value);
    return column;
}

/// 1000 rows drawn from `keys`, with runs of repeated values.
std::vector<Field> makeSourceValues(const std::vector<Field> & keys, UInt64 seed)
{
    pcg64 rng(seed);
    std::vector<Field> values;
    while (values.size() < 1000)
    {
        const Field & key = keys[rng() % keys.size()];
        for (size_t repeat = 1 + rng() % 3; repeat > 0 && values.size() < 1000; --repeat)
            values.push_back(key);
    }
    return values;
}

/// A range short enough for the per-row path must give the same rows and the same dictionary prefix
/// as a range of the same rows that is long enough for the range path.
void checkShortRangeMatchesRangePath(const IColumn & source, const std::function<MutableColumnPtr()> & make_destination)
{
    const auto & source_lc = assert_cast<const ColumnLowCardinality &>(source);
    const auto & source_keys = *source_lc.getDictionary().getNestedNotNullableColumn();
    constexpr size_t long_length = 600;
    for (size_t start : {0, 1, 7, 300})
    {
        for (size_t length = 1; length <= 256; ++length)
        {
            auto short_range = make_destination();
            auto long_range = make_destination();
            const size_t offset = short_range->size();
            short_range->insertRangeFrom(source, start, length);
            long_range->insertRangeFrom(source, start, long_length);

            const auto & short_lc = assert_cast<const ColumnLowCardinality &>(*short_range);
            const auto & long_lc = assert_cast<const ColumnLowCardinality &>(*long_range);
            ASSERT_EQ(short_range->size(), offset + length);
            for (size_t row = offset; row < offset + length; ++row)
            {
                SCOPED_TRACE(fmt::format("start {}, length {}, row {}", start, length, row));
                ASSERT_EQ(short_lc.getIndexes().getUInt(row), long_lc.getIndexes().getUInt(row));
                ASSERT_EQ(short_lc.isNullAt(row), long_lc.isNullAt(row));
                if (!short_lc.isNullAt(row))
                    ASSERT_EQ(short_lc.getDataAt(row), long_lc.getDataAt(row));

                /// Both paths apply the same NULL and default rules, so check those against the source too.
                const size_t source_row = start + row - offset;
                const auto & dictionary = short_lc.getDictionary();
                const size_t default_index = dictionary.getNestedTypeDefaultValueIndex();
                if (source.isNullAt(source_row))
                    ASSERT_EQ(short_lc.getIndexes().getUInt(row), dictionary.getNullValueIndex());
                else if (dictionary.getNestedNotNullableColumn()->compareAt(
                             default_index, source_lc.getIndexes().getUInt(source_row), source_keys, 1) == 0)
                    ASSERT_EQ(short_lc.getIndexes().getUInt(row), default_index);
            }

            const auto & short_keys = *short_lc.getDictionary().getNestedNotNullableColumn();
            const auto & long_keys = *long_lc.getDictionary().getNestedNotNullableColumn();
            ASSERT_LE(short_keys.size(), long_keys.size());
            for (size_t key = 0; key < short_keys.size(); ++key)
                ASSERT_EQ(short_keys.getDataAt(key), long_keys.getDataAt(key)) << "start " << start << ", length " << length;
        }
    }
}

}

TEST(ColumnLowCardinality, ShortRangeFromDifferentDictionaryMatchesRangePath)
{
    const auto string_type = std::make_shared<DataTypeString>();
    const auto nullable_string_type = std::make_shared<DataTypeNullable>(string_type);

    std::vector<Field> string_keys{Field(""), Field("a"), Field("b"), Field("c"), Field("d"), Field("e"), Field(std::string(40, 'f'))};
    auto string_source = makeLowCardinality(string_type, makeSourceValues(string_keys, 1));
    auto make_string_destination = [&] { return makeLowCardinality(string_type, {Field("b"), Field("x"), Field(""), Field("a"), Field("y")}); };

    {
        SCOPED_TRACE("LowCardinality(String)");
        ASSERT_NO_FATAL_FAILURE(checkShortRangeMatchesRangePath(*string_source, make_string_destination));
    }
    {
        SCOPED_TRACE("LowCardinality(Nullable(String))");
        std::vector<Field> nullable_keys = string_keys;
        nullable_keys.push_back(Field());
        auto source = makeLowCardinality(nullable_string_type, makeSourceValues(nullable_keys, 2));
        auto make_destination = [&] { return makeLowCardinality(nullable_string_type, {Field("b"), Field(), Field("x")}); };
        ASSERT_NO_FATAL_FAILURE(checkShortRangeMatchesRangePath(*source, make_destination));
    }
    {
        SCOPED_TRACE("LowCardinality(String) into LowCardinality(Nullable(String))");
        auto make_destination = [&] { return makeLowCardinality(nullable_string_type, {Field("b"), Field(), Field("x")}); };
        ASSERT_NO_FATAL_FAILURE(checkShortRangeMatchesRangePath(*string_source, make_destination));
    }
    {
        SCOPED_TRACE("Shared source dictionary");
        const auto & string_source_lc = assert_cast<const ColumnLowCardinality &>(*string_source);
        auto source = ColumnLowCardinality::create(string_source_lc.getDictionaryPtr(), string_source_lc.getIndexesPtr(), /* is_shared = */ true);
        ASSERT_NO_FATAL_FAILURE(checkShortRangeMatchesRangePath(*source, make_string_destination));
    }
    {
        /// The keys are set directly, so that the dictionary holds `-0.0` next to `0.0` and two NaN payloads.
        SCOPED_TRACE("LowCardinality(Float64)");
        auto keys = ColumnFloat64::create();
        for (Float64 key : {0.0, -0.0, 1.5, std::bit_cast<Float64>(0x7ff8000000000001ULL), std::bit_cast<Float64>(0xfff8000000000002ULL)})
            keys->insertValue(key);
        ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeFloat64(), std::move(keys));
        auto indexes = ColumnUInt8::create();
        pcg64 rng(3);
        for (size_t row = 0; row < 1000; ++row)
            indexes->insertValue(static_cast<UInt8>(rng() % 5));
        auto source = ColumnLowCardinality::create(dictionary, std::move(indexes), /* is_shared = */ false);
        auto make_destination = [&] { return makeLowCardinality(std::make_shared<DataTypeFloat64>(), {Field(2.5), Field(1.5)}); };
        ASSERT_NO_FATAL_FAILURE(checkShortRangeMatchesRangePath(*source, make_destination));
    }
    {
        SCOPED_TRACE("LowCardinality(UInt64), index type grows");
        const auto uint64_type = std::make_shared<DataTypeUInt64>();
        std::vector<Field> keys;
        for (UInt64 key = 0; key < 300; ++key)
            keys.push_back(Field(key * 10));
        auto source = makeLowCardinality(uint64_type, makeSourceValues(keys, 4));
        std::vector<Field> destination_values;
        for (UInt64 value = 1; value <= 250; ++value)
            destination_values.push_back(Field(value * 7));
        auto make_destination = [&] { return makeLowCardinality(uint64_type, destination_values); };
        ASSERT_EQ(assert_cast<const ColumnLowCardinality &>(*make_destination()).getSizeOfIndexType(), sizeof(UInt8));
        ASSERT_NO_FATAL_FAILURE(checkShortRangeMatchesRangePath(*source, make_destination));

        auto destination = make_destination();
        destination->insertRangeFrom(*source, 0, 32);
        ASSERT_EQ(assert_cast<const ColumnLowCardinality &>(*destination).getSizeOfIndexType(), sizeof(UInt16));
    }
    {
        SCOPED_TRACE("Out of bound range");
        auto destination = make_string_destination();
        const size_t size_before = destination->size();
        const size_t dictionary_size_before = assert_cast<const ColumnLowCardinality &>(*destination).getDictionary().size();
        EXPECT_THROW(destination->insertRangeFrom(*string_source, string_source->size() - 1, 2), Exception);
        EXPECT_THROW(destination->insertRangeFrom(*string_source, std::numeric_limits<size_t>::max(), 1), Exception);
        EXPECT_EQ(destination->size(), size_before);
        EXPECT_EQ(assert_cast<const ColumnLowCardinality &>(*destination).getDictionary().size(), dictionary_size_before);
    }
}
