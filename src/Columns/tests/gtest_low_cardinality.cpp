#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <gtest/gtest.h>
#include <Common/Exception.h>

#include <algorithm>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
}

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

TEST(ColumnLowCardinality, InsertRangeFromChecksBoundsBeforeSharingDictionary)
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
    const auto * original_dictionary = &low_cardinality_destination.getDictionary();

    ASSERT_EQ(low_cardinality_destination.getSizeOfIndexType(), sizeof(UInt16));
    ASSERT_NE(original_dictionary, &source->getDictionary());
    try
    {
        destination->insertRangeFrom(*source, source->size(), 1);
        FAIL() << "Expected PARAMETER_OUT_OF_BOUND for an invalid source range";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }
    EXPECT_TRUE(destination->empty());
    EXPECT_EQ(&low_cardinality_destination.getDictionary(), original_dictionary);
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_EQ(low_cardinality_destination.getSizeOfIndexType(), sizeof(UInt16));
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

template <typename IndexType>
ColumnLowCardinality::MutablePtr makeLowCardinalityUInt64Column(
    size_t dictionary_size,
    const std::vector<IndexType> & index_values,
    UInt64 key_offset = 0,
    bool is_shared = false)
{
    auto keys = ColumnUInt64::create(dictionary_size);
    auto & key_data = keys->getData();
    key_data[0] = 0;
    for (size_t i = 1; i < dictionary_size; ++i)
        key_data[i] = key_offset + i;

    auto dictionary_type = std::make_shared<DataTypeUInt64>();
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(keys));
    MutableColumnPtr indexes = ColumnVector<IndexType>::create();
    assert_cast<ColumnVector<IndexType> &>(*indexes).getData().assign(index_values.begin(), index_values.end());

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), is_shared);
}

template <typename IndexType>
ColumnLowCardinality::MutablePtr makeLowCardinalityUInt128Column(
    size_t dictionary_size, const std::vector<IndexType> & index_values, UInt128 key_offset = 0)
{
    auto keys = ColumnUInt128::create(dictionary_size);
    auto & key_data = keys->getData();
    key_data[0] = 0;
    for (size_t i = 1; i < dictionary_size; ++i)
        key_data[i] = key_offset + i;

    auto dictionary_type = std::make_shared<DataTypeUInt128>();
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(keys));
    MutableColumnPtr indexes = ColumnVector<IndexType>::create();
    assert_cast<ColumnVector<IndexType> &>(*indexes).getData().assign(index_values.begin(), index_values.end());

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/false);
}

template <typename IndexType>
ColumnLowCardinality::MutablePtr makeNullableLowCardinalityUInt128Column(
    size_t dictionary_size, const std::vector<IndexType> & index_values, UInt128 key_offset = 0)
{
    auto keys = ColumnUInt128::create(dictionary_size);
    auto & key_data = keys->getData();
    key_data[0] = 0;
    key_data[1] = 0;
    for (size_t i = 2; i < dictionary_size; ++i)
        key_data[i] = key_offset + i;

    auto dictionary_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt128>());
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(keys));
    MutableColumnPtr indexes = ColumnVector<IndexType>::create();
    assert_cast<ColumnVector<IndexType> &>(*indexes).getData().assign(index_values.begin(), index_values.end());

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/false);
}

template <typename IndexType>
ColumnLowCardinality::MutablePtr makeLowCardinalityStringColumn(
    size_t dictionary_size, const std::vector<IndexType> & index_values, const String & key_prefix)
{
    auto keys = ColumnString::create();
    keys->insertDefault();
    for (size_t i = 1; i < dictionary_size; ++i)
    {
        const String key = key_prefix + std::to_string(i);
        keys->insertData(key.data(), key.size());
    }

    auto dictionary_type = std::make_shared<DataTypeString>();
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(keys));
    MutableColumnPtr indexes = ColumnVector<IndexType>::create();
    assert_cast<ColumnVector<IndexType> &>(*indexes).getData().assign(index_values.begin(), index_values.end());

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/false);
}

template <typename IndexType>
ColumnLowCardinality::MutablePtr makeNullableLowCardinalityUInt64Column(
    size_t dictionary_size, const std::vector<IndexType> & index_values, bool is_shared = false)
{
    auto keys = ColumnUInt64::create(dictionary_size);
    auto & key_data = keys->getData();
    for (size_t i = 0; i < dictionary_size; ++i)
        key_data[i] = i;
    /// Nullable dictionaries reserve index 0 for NULL and index 1 for the nested default.
    key_data[0] = 0;
    key_data[1] = 0;

    auto dictionary_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(keys));
    MutableColumnPtr indexes = ColumnVector<IndexType>::create();
    assert_cast<ColumnVector<IndexType> &>(*indexes).getData().assign(index_values.begin(), index_values.end());

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), is_shared);
}

template <typename IndexType>
void checkMinimalDictionary(
    const std::vector<IndexType> & original_indexes, size_t offset, size_t length)
{
    const auto max_index = original_indexes.empty()
        ? IndexType{0}
        : *std::max_element(original_indexes.begin(), original_indexes.end());
    auto column = makeLowCardinalityUInt64Column<IndexType>(static_cast<size_t>(max_index) + 1, original_indexes);

    const auto minimal = column->getMinimalDictionaryEncodedColumn(offset, length);
    const auto * rewritten_indexes = typeid_cast<const ColumnVector<IndexType> *>(minimal.indexes.get());
    ASSERT_NE(rewritten_indexes, nullptr);

    std::vector<IndexType> first_seen;
    std::vector<IndexType> expected_indexes;
    for (size_t i = offset; i < offset + length; ++i)
    {
        const auto source_index = original_indexes[i];
        const auto it = std::find(first_seen.begin(), first_seen.end(), source_index);
        if (it == first_seen.end())
        {
            expected_indexes.push_back(static_cast<IndexType>(first_seen.size()));
            first_seen.push_back(source_index);
        }
        else
            expected_indexes.push_back(static_cast<IndexType>(it - first_seen.begin()));
    }

    ASSERT_EQ(minimal.dictionary->size(), first_seen.size());
    ASSERT_EQ(rewritten_indexes->size(), expected_indexes.size());
    for (size_t i = 0; i < first_seen.size(); ++i)
        EXPECT_EQ(minimal.dictionary->getUInt(i), first_seen[i]);
    for (size_t i = 0; i < expected_indexes.size(); ++i)
    {
        EXPECT_EQ(rewritten_indexes->getData()[i], expected_indexes[i]);
        EXPECT_EQ(
            minimal.dictionary->getUInt(rewritten_indexes->getData()[i]),
            original_indexes[offset + i]);
    }
}

template <typename IndexType>
void checkSparseMinimalDictionaryForIndexType()
{
    /// Nonzero offset, repeated indexes, a later zero, and first occurrences in nonsorted order.
    checkMinimalDictionary<IndexType>({17, 200, 5, 200, 0, 130, 5, 17}, 1, 6);
    /// Zero first, zero absent, one distinct key, and all distinct keys.
    checkMinimalDictionary<IndexType>({0, 200, 0}, 0, 3);
    checkMinimalDictionary<IndexType>({200, 5, 130}, 0, 3);
    checkMinimalDictionary<IndexType>({200, 200, 200}, 0, 3);
    checkMinimalDictionary<IndexType>({200, 130, 5}, 0, 3);
    /// Empty range and a range ending exactly at the source boundary.
    checkMinimalDictionary<IndexType>({17, 200, 5}, 2, 0);
    checkMinimalDictionary<IndexType>({17, 200, 5}, 1, 2);
}

void expectRangeEquals(
    const IColumn & destination,
    size_t destination_offset,
    const IColumn & source,
    size_t source_offset,
    size_t length)
{
    ASSERT_LE(destination_offset + length, destination.size());
    ASSERT_LE(source_offset + length, source.size());
    for (size_t i = 0; i < length; ++i)
        EXPECT_EQ(destination[destination_offset + i], source[source_offset + i]);
}

}

TEST(ColumnLowCardinality, SparseMinimalDictionaryAllIndexTypes)
{
    checkSparseMinimalDictionaryForIndexType<UInt8>();
    checkSparseMinimalDictionaryForIndexType<UInt16>();
    checkSparseMinimalDictionaryForIndexType<UInt32>();
    checkSparseMinimalDictionaryForIndexType<UInt64>();
}

TEST(ColumnLowCardinality, SparseMinimalDictionaryNullable)
{
    auto column = makeNullableLowCardinalityUInt64Column<UInt16>(201, {200, 0, 2, 200, 1});

    const auto minimal = column->getMinimalDictionaryEncodedColumn(0, column->size());
    const auto * rewritten_indexes = typeid_cast<const ColumnUInt16 *>(minimal.indexes.get());
    ASSERT_NE(rewritten_indexes, nullptr);
    ASSERT_EQ(rewritten_indexes->getData(), ColumnUInt16::Container({0, 1, 2, 0, 3}));
    ASSERT_EQ(minimal.dictionary->size(), 4);
    EXPECT_EQ(minimal.dictionary->getUInt(0), 200);
    EXPECT_TRUE(minimal.dictionary->isNullAt(1));
    EXPECT_EQ(minimal.dictionary->getUInt(2), 2);
    EXPECT_EQ(minimal.dictionary->getUInt(3), 0);
}

TEST(ColumnLowCardinality, InsertSparseRangeFromDifferentDictionary)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(256, {17, 200, 5, 200, 0, 130, 5, 17});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    destination->insert(UInt64{999});

    destination->insertRangeFrom(*source, 1, 6);

    ASSERT_EQ(destination->size(), 7);
    EXPECT_EQ(destination->getUInt(0), 999);
    for (size_t i = 0; i < 6; ++i)
        EXPECT_EQ((*destination)[i + 1], (*source)[i + 1]);
}

TEST(ColumnLowCardinality, InsertSparseRangeFromChecksBoundsBeforeReadingSourceIndexes)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(256, {17, 200, 5});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    destination->insert(UInt64{999});

    try
    {
        destination->insertRangeFrom(*source, source->size(), 1);
        FAIL() << "Expected PARAMETER_OUT_OF_BOUND for an invalid source range";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }
    ASSERT_EQ(destination->size(), 1);
    EXPECT_EQ(destination->getUInt(0), 999);
}

TEST(ColumnLowCardinality, EmptyDestinationSharesMinimalSourceDictionary)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(
        5, {4, 1, 3, 2, 4, 2, 1}, /*key_offset=*/0, /*is_shared=*/true);
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());

    auto full_destination = low_cardinality_type->createColumn();
    full_destination->insertRangeFrom(*source, 0, source->size());
    const auto & full_low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*full_destination);
    EXPECT_EQ(&full_low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_TRUE(full_low_cardinality_destination.isSharedDictionary());

    auto sliced_destination = low_cardinality_type->createColumn();
    sliced_destination->insertRangeFrom(*source, 1, 4);
    const auto & sliced_low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*sliced_destination);
    EXPECT_EQ(&sliced_low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_TRUE(sliced_low_cardinality_destination.isSharedDictionary());
    EXPECT_EQ(sliced_low_cardinality_destination.getSizeOfIndexType(), sizeof(UInt32));
    for (size_t i = 0; i < 4; ++i)
        EXPECT_EQ((*sliced_destination)[i], (*source)[i + 1]);

    sliced_destination->insertRangeFrom(*source, 5, 2);
    EXPECT_EQ(&sliced_low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_TRUE(sliced_low_cardinality_destination.isSharedDictionary());
    for (size_t i = 0; i < 2; ++i)
        EXPECT_EQ((*sliced_destination)[4 + i], (*source)[5 + i]);
}

TEST(ColumnLowCardinality, EmptyDestinationDoesNotShareUnsharedMinimalSourceDictionary)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(4, {3, 1, 3, 0, 2});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    const size_t source_size = source->size();

    destination->insertRangeFrom(*source, 0, source_size);

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    const size_t destination_dictionary_size = low_cardinality_destination.getDictionary().size();

    source->insert(UInt64{999});

    EXPECT_EQ(low_cardinality_destination.getDictionary().size(), destination_dictionary_size);
    EXPECT_EQ(destination->size(), source_size);
    for (size_t i = 0; i < source_size; ++i)
        EXPECT_EQ((*destination)[i], (*source)[i]);
    EXPECT_EQ(source->getUInt(source_size), 999);
}

TEST(ColumnLowCardinality, EmptyDestinationDoesNotShareNonMinimalSourceDictionary)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(256, {200});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();

    destination->insertRangeFrom(*source, 0, source->size());

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_EQ(low_cardinality_destination.getDictionary().size(), 2);
    EXPECT_EQ(destination->getUInt(0), source->getUInt(0));
}

TEST(ColumnLowCardinality, EmptyDestinationDoesNotShareRangeThatOmitsDictionaryKey)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(5, {1, 2, 3, 3});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();

    destination->insertRangeFrom(*source, 0, source->size());

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_EQ(low_cardinality_destination.getDictionary().size(), 4);
    expectRangeEquals(*destination, 0, *source, 0, source->size());
}

TEST(ColumnLowCardinality, EmptyNonNullableDestinationDoesNotShareNullableSourceDictionary)
{
    auto source = makeNullableLowCardinalityUInt64Column<UInt32>(256, {200, 1, 5});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();

    destination->insertRangeFrom(*source, 0, source->size());

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_FALSE(low_cardinality_destination.nestedIsNullable());
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    ASSERT_EQ(destination->size(), source->size());
    for (size_t i = 0; i < source->size(); ++i)
        EXPECT_EQ((*destination)[i], (*source)[i]);
}

TEST(ColumnLowCardinality, EmptyNullableDestinationDoesNotShareNonNullableSourceDictionary)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(256, {0, 200, 5});
    auto nested_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(nested_type);
    auto destination = low_cardinality_type->createColumn();

    destination->insertRangeFrom(*source, 0, source->size());

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_TRUE(low_cardinality_destination.nestedIsNullable());
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    ASSERT_EQ(destination->size(), source->size());
    for (size_t i = 0; i < source->size(); ++i)
    {
        EXPECT_FALSE(destination->isNullAt(i));
        EXPECT_EQ((*destination)[i], (*source)[i]);
    }
}

TEST(ColumnLowCardinality, EmptyRangeDoesNotShareSourceDictionary)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(256, {17, 200, 5});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    const auto * original_dictionary = &low_cardinality_destination.getDictionary();

    destination->insertRangeFrom(*source, source->size(), 0);

    EXPECT_TRUE(destination->empty());
    EXPECT_EQ(&low_cardinality_destination.getDictionary(), original_dictionary);
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
}

TEST(ColumnLowCardinality, SharedDestinationCompactsForDifferentDictionary)
{
    auto first_source = makeLowCardinalityUInt64Column<UInt32>(
        3, {0, 2, 1, 2, 0}, /*key_offset=*/0, /*is_shared=*/true);
    auto second_source = makeLowCardinalityUInt64Column<UInt16>(256, {17, 5, 250, 17});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();

    destination->insertRangeFrom(*first_source, 1, 3);
    destination->insertRangeFrom(*second_source, 0, second_source->size());

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &first_source->getDictionary());
    ASSERT_EQ(destination->size(), 3 + second_source->size());
    for (size_t i = 0; i < 3; ++i)
        EXPECT_EQ((*destination)[i], (*first_source)[i + 1]);
    for (size_t i = 0; i < second_source->size(); ++i)
        EXPECT_EQ((*destination)[3 + i], (*second_source)[i]);
}

TEST(ColumnLowCardinality, MutationAfterSharingDoesNotChangeSource)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(
        4, {3, 1, 3, 0, 2}, /*key_offset=*/0, /*is_shared=*/true);
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    const size_t source_dictionary_size = source->getDictionary().size();

    destination->insertRangeFrom(*source, 0, source->size());
    destination->insert(UInt64{999});

    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_EQ(source->getDictionary().size(), source_dictionary_size);
    EXPECT_EQ(destination->getUInt(destination->size() - 1), 999);
    for (size_t i = 0; i < source->size(); ++i)
        EXPECT_EQ((*destination)[i], (*source)[i]);
}

TEST(ColumnLowCardinality, SourceMutationAfterSharingDoesNotChangeDestination)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(
        4, {3, 1, 3, 0, 2}, /*key_offset=*/0, /*is_shared=*/true);
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    const size_t source_size = source->size();

    destination->insertRangeFrom(*source, 0, source_size);
    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    ASSERT_EQ(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    ASSERT_TRUE(low_cardinality_destination.isSharedDictionary());
    ASSERT_TRUE(source->isSharedDictionary());
    const size_t destination_dictionary_size = low_cardinality_destination.getDictionary().size();

    source->insert(UInt64{999});

    EXPECT_NE(&low_cardinality_destination.getDictionary(), &source->getDictionary());
    EXPECT_EQ(low_cardinality_destination.getDictionary().size(), destination_dictionary_size);
    EXPECT_EQ(destination->size(), source_size);
    for (size_t i = 0; i < source_size; ++i)
        EXPECT_EQ((*destination)[i], (*source)[i]);
    EXPECT_EQ(source->getUInt(source_size), 999);
}

TEST(ColumnLowCardinality, NullableDefaultsSurviveSharingAndCompaction)
{
    auto first_source = makeNullableLowCardinalityUInt64Column<UInt32>(
        4, {0, 1, 3, 0, 2}, /*is_shared=*/true);
    auto second_source = makeNullableLowCardinalityUInt64Column<UInt16>(256, {1, 0, 150});
    auto nested_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(nested_type);
    auto destination = low_cardinality_type->createColumn();

    destination->insertRangeFrom(*first_source, 0, first_source->size());
    const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
    EXPECT_EQ(&low_cardinality_destination.getDictionary(), &first_source->getDictionary());
    destination->insertRangeFrom(*second_source, 0, second_source->size());

    EXPECT_FALSE(low_cardinality_destination.isSharedDictionary());
    EXPECT_TRUE(destination->isNullAt(0));
    EXPECT_FALSE(destination->isNullAt(1));
    EXPECT_EQ(destination->getUInt(1), 0);
    EXPECT_TRUE(destination->isNullAt(first_source->size() + 1));
    for (size_t i = 0; i < first_source->size(); ++i)
        EXPECT_EQ((*destination)[i], (*first_source)[i]);
    for (size_t i = 0; i < second_source->size(); ++i)
        EXPECT_EQ((*destination)[first_source->size() + i], (*second_source)[i]);
}

TEST(ColumnLowCardinality, TranslateSparseDifferentDictionaries)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(256, {200, 5, 200, 0, 130});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());

    auto disjoint_destination = low_cardinality_type->createColumn();
    disjoint_destination->insert(UInt64{999});
    disjoint_destination->insertRangeFrom(*source, 0, source->size());
    expectRangeEquals(*disjoint_destination, 1, *source, 0, source->size());

    auto overlapping_destination = low_cardinality_type->createColumn();
    overlapping_destination->insert(UInt64{200});
    overlapping_destination->insert(UInt64{5});
    overlapping_destination->insert(UInt64{0});
    overlapping_destination->insert(UInt64{130});
    overlapping_destination->insertRangeFrom(*source, 0, source->size());
    expectRangeEquals(*overlapping_destination, 4, *source, 0, source->size());

    auto partially_overlapping_destination = low_cardinality_type->createColumn();
    partially_overlapping_destination->insert(UInt64{999});
    partially_overlapping_destination->insert(UInt64{200});
    partially_overlapping_destination->insertRangeFrom(*source, 0, source->size());
    expectRangeEquals(*partially_overlapping_destination, 2, *source, 0, source->size());
}

TEST(ColumnLowCardinality, TranslateSparseHighUInt32IndexesWithOffset)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(100000, {1, 99999, 70000, 99999, 50000});
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    destination->insert(UInt64{200000});

    destination->insertRangeFrom(*source, 1, 4);

    ASSERT_EQ(destination->size(), 5);
    expectRangeEquals(*destination, 1, *source, 1, 4);
}

TEST(ColumnLowCardinality, TranslateSparseIndexesPromotesDestinationType)
{
    auto source = makeLowCardinalityUInt64Column<UInt32>(256, {200, 201}, 1000000);

    auto destination_uint8 = makeLowCardinalityUInt64Column<UInt8>(255, {1});
    destination_uint8->insertRangeFrom(*source, 0, source->size());
    EXPECT_EQ(destination_uint8->getSizeOfIndexType(), sizeof(UInt16));
    expectRangeEquals(*destination_uint8, 1, *source, 0, source->size());

    auto destination_uint16 = makeLowCardinalityUInt64Column<UInt16>(65535, {1});
    destination_uint16->insertRangeFrom(*source, 0, source->size());
    EXPECT_EQ(destination_uint16->getSizeOfIndexType(), sizeof(UInt32));
    expectRangeEquals(*destination_uint16, 1, *source, 0, source->size());

    auto destination_uint32 = makeLowCardinalityUInt64Column<UInt32>(70000, {1});
    destination_uint32->insertRangeFrom(*source, 0, source->size());
    EXPECT_EQ(destination_uint32->getSizeOfIndexType(), sizeof(UInt32));
    expectRangeEquals(*destination_uint32, 1, *source, 0, source->size());
}

TEST(ColumnLowCardinality, TranslateSparseIndexesFromAlternatingDictionaries)
{
    auto first_source = makeLowCardinalityUInt64Column<UInt32>(256, {200, 5, 200, 130}, 1000);
    auto second_source = makeLowCardinalityUInt64Column<UInt16>(256, {250, 17, 250}, 2000);
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto destination = low_cardinality_type->createColumn();
    destination->insert(UInt64{999999});

    destination->insertRangeFrom(*first_source, 0, first_source->size());
    destination->insertRangeFrom(*second_source, 0, second_source->size());
    destination->insertRangeFrom(*first_source, 1, 2);

    size_t offset = 1;
    expectRangeEquals(*destination, offset, *first_source, 0, first_source->size());
    offset += first_source->size();
    expectRangeEquals(*destination, offset, *second_source, 0, second_source->size());
    offset += second_source->size();
    expectRangeEquals(*destination, offset, *first_source, 1, 2);
}

TEST(ColumnLowCardinality, TranslateSparseIndexesForNestedTypes)
{
    {
        auto source = makeLowCardinalityUInt128Column<UInt32>(256, {200, 5, 200, 130}, UInt128{1} << 100);
        auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt128>());
        auto destination = low_cardinality_type->createColumn();
        destination->insert(UInt128{999});
        destination->insertRangeFrom(*source, 0, source->size());
        expectRangeEquals(*destination, 1, *source, 0, source->size());
    }

    {
        auto source = makeLowCardinalityStringColumn<UInt16>(256, {200, 5, 200, 130}, "source_");
        auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        auto destination = low_cardinality_type->createColumn();
        destination->insert(String{"seed"});
        destination->insertRangeFrom(*source, 0, source->size());
        expectRangeEquals(*destination, 1, *source, 0, source->size());
    }

    {
        auto source = makeNullableLowCardinalityUInt128Column<UInt32>(256, {200, 0, 1, 200}, UInt128{1} << 100);
        auto nested_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt128>());
        auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(nested_type);
        auto destination = low_cardinality_type->createColumn();
        destination->insert(Field(Null()));
        destination->insertRangeFrom(*source, 0, source->size());
        expectRangeEquals(*destination, 1, *source, 0, source->size());
        EXPECT_TRUE(destination->isNullAt(2));
        EXPECT_FALSE(destination->isNullAt(3));
    }
}

TEST(ColumnLowCardinality, TranslateSparseIndexesInsideArray)
{
    MutableColumnPtr source_nested = makeLowCardinalityUInt128Column<UInt32>(
        256, {200, 5, 200, 130}, UInt128{1} << 100);
    MutableColumnPtr source_offsets = ColumnArray::ColumnOffsets::create();
    auto & source_offset_data = assert_cast<ColumnArray::ColumnOffsets &>(*source_offsets).getData();
    source_offset_data.push_back(UInt64{2});
    source_offset_data.push_back(UInt64{4});
    MutableColumnPtr source = ColumnArray::create(std::move(source_nested), std::move(source_offsets));

    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt128>());
    MutableColumnPtr destination_nested = low_cardinality_type->createColumn();
    destination_nested->insert(UInt128{999});
    MutableColumnPtr destination_offsets = ColumnArray::ColumnOffsets::create();
    assert_cast<ColumnArray::ColumnOffsets &>(*destination_offsets).getData().push_back(UInt64{1});
    MutableColumnPtr destination = ColumnArray::create(std::move(destination_nested), std::move(destination_offsets));

    destination->insertRangeFrom(*source, 0, source->size());

    ASSERT_EQ(destination->size(), 3);
    EXPECT_EQ((*destination)[1], (*source)[0]);
    EXPECT_EQ((*destination)[2], (*source)[1]);
}
