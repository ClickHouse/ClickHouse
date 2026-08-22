#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>

#include <cmath>
#include <initializer_list>

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

TEST(ColumnLowCardinality, InsertionsIntoCloneEmptyPreserveSingleDictionary)
{
    auto dictionary_keys = ColumnUInt64::create();
    for (UInt64 value : {0, 10})
        dictionary_keys->insertValue(value);

    ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeUInt64(), std::move(dictionary_keys));

    auto source_indexes = ColumnUInt8::create();
    source_indexes->insertValue(1);
    auto source = ColumnLowCardinality::create(
        dictionary,
        std::move(source_indexes),
        /* is_shared = */ true,
        /* has_single_dictionary_for_part = */ true);

    auto insert_from_destination = source->cloneEmpty();
    insert_from_destination->insertFrom(*source, 0);
    const auto & insert_from_low_cardinality = assert_cast<const ColumnLowCardinality &>(*insert_from_destination);

    EXPECT_EQ(insert_from_low_cardinality.getDictionaryPtr().get(), dictionary.get());
    EXPECT_TRUE(insert_from_low_cardinality.isSharedDictionary());
    EXPECT_TRUE(insert_from_low_cardinality.hasSingleDictionaryForPart());
    EXPECT_EQ(insert_from_low_cardinality.getUInt(0), 10);

    auto insert_many_from_destination = source->cloneEmpty();
    insert_many_from_destination->insertManyFrom(*source, 0, 3);
    const auto & insert_many_from_low_cardinality = assert_cast<const ColumnLowCardinality &>(*insert_many_from_destination);

    EXPECT_EQ(insert_many_from_low_cardinality.getDictionaryPtr().get(), dictionary.get());
    EXPECT_TRUE(insert_many_from_low_cardinality.isSharedDictionary());
    EXPECT_TRUE(insert_many_from_low_cardinality.hasSingleDictionaryForPart());
    ASSERT_EQ(insert_many_from_low_cardinality.size(), 3);
    EXPECT_EQ(insert_many_from_low_cardinality.getUInt(0), 10);
    EXPECT_EQ(insert_many_from_low_cardinality.getUInt(1), 10);
    EXPECT_EQ(insert_many_from_low_cardinality.getUInt(2), 10);
}

TEST(ColumnLowCardinality, InsertionsAfterDefaultsPreserveSingleDictionary)
{
    for (bool is_nullable : {false, true})
    {
        SCOPED_TRACE(is_nullable ? "nullable" : "nonnullable");

        DataTypePtr data_type = std::make_shared<DataTypeUInt64>();
        if (is_nullable)
            data_type = std::make_shared<DataTypeNullable>(data_type);

        auto dictionary_keys = ColumnUInt64::create();
        dictionary_keys->insertDefault();
        if (is_nullable)
            dictionary_keys->insertDefault();
        /// Leave an unused key before the copied value to detect dictionary index renumbering.
        dictionary_keys->insertValue(20);
        dictionary_keys->insertValue(10);

        ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*data_type, std::move(dictionary_keys));
        const auto value_index = static_cast<UInt8>(dictionary->size() - 1);
        auto source_indexes = ColumnUInt8::create();
        source_indexes->insertValue(value_index);
        source_indexes->insertValue(0);
        source_indexes->insertValue(value_index);
        auto source = ColumnLowCardinality::create(
            dictionary,
            std::move(source_indexes),
            /* is_shared = */ true,
            /* has_single_dictionary_for_part = */ true);

        auto check_insertion = [&](const char * name, auto insert_rows, std::initializer_list<size_t> source_rows)
        {
            SCOPED_TRACE(name);
            for (size_t default_rows : {0, 1, 3})
            {
                SCOPED_TRACE(default_rows);
                auto destination = source->cloneEmpty();
                const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(*destination);
                destination->insertManyDefaults(default_rows);
                ASSERT_FALSE(low_cardinality.isSharedDictionary());
                ASSERT_FALSE(low_cardinality.hasSingleDictionaryForPart());

                /// A leading blank in `JoinCommon::filterWithBlanks` precedes the first copied row.
                insert_rows(*destination);

                ASSERT_EQ(destination->size(), default_rows + source_rows.size());
                EXPECT_EQ(low_cardinality.getDictionaryPtr().get(), dictionary.get());
                EXPECT_TRUE(low_cardinality.isSharedDictionary());
                EXPECT_TRUE(low_cardinality.hasSingleDictionaryForPart());
                for (size_t row = 0; row < default_rows; ++row)
                {
                    EXPECT_EQ(low_cardinality.getIndexes().getUInt(row), 0);
                    EXPECT_EQ((*destination)[row], data_type->getDefault());
                }
                size_t destination_row = default_rows;
                for (size_t source_row : source_rows)
                {
                    EXPECT_EQ(low_cardinality.getIndexes().getUInt(destination_row), source->getIndexes().getUInt(source_row));
                    EXPECT_EQ((*destination)[destination_row], (*source)[source_row]);
                    ++destination_row;
                }
            }
        };

        check_insertion("insertFrom", [&](IColumn & destination)
        {
            destination.insertFrom(*source, 0);
        }, {0});
        check_insertion("insertManyFrom", [&](IColumn & destination)
        {
            destination.insertManyFrom(*source, 0, 3);
        }, {0, 0, 0});
        check_insertion("insertRangeFrom", [&](IColumn & destination)
        {
            destination.insertRangeFrom(*source, 0, source->size());
        }, {0, 1, 2});
    }
}

TEST(ColumnLowCardinality, InsertionsAfterDefaultsRespectDictionaryCompatibility)
{
    struct Case
    {
        const char * name = nullptr;
        bool source_is_shared = true;
        bool source_has_proof = true;
        Float64 source_default = 0;
        bool destination_is_nullable = false;
        Float64 destination_value = 0;
        bool expect_shared = false;
    };

    const Case cases[] =
    {
        {.name = "private source", .source_is_shared = false, .source_has_proof = false},
        {.name = "unverified source", .source_has_proof = false, .expect_shared = true},
        {.name = "nondefault destination", .destination_value = 99},
        {.name = "nullable promotion", .destination_is_nullable = true},
        {.name = "different default encoding", .source_default = -0.0},
    };

    auto check_insertion = [&](const char * name, auto insert_row)
    {
        SCOPED_TRACE(name);
        for (const auto & test_case : cases)
        {
            SCOPED_TRACE(test_case.name);
            auto dictionary_keys = ColumnFloat64::create();
            dictionary_keys->insertValue(test_case.source_default);
            dictionary_keys->insertValue(20);
            dictionary_keys->insertValue(10);
            ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeFloat64(), std::move(dictionary_keys));
            auto source_indexes = ColumnUInt8::create();
            source_indexes->insertValue(2);
            auto source = ColumnLowCardinality::create(
                dictionary, std::move(source_indexes), test_case.source_is_shared, test_case.source_has_proof);

            DataTypePtr data_type = std::make_shared<DataTypeFloat64>();
            if (test_case.destination_is_nullable)
                data_type = std::make_shared<DataTypeNullable>(data_type);
            auto destination = DataTypeLowCardinality(data_type).createColumn();
            if (test_case.destination_value == 0)
                destination->insertDefault();
            else
                destination->insert(Field{test_case.destination_value});

            insert_row(*destination, *source);

            const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(*destination);
            ASSERT_EQ(destination->size(), 2);
            EXPECT_EQ(low_cardinality.getDictionaryPtr().get() == dictionary.get(), test_case.expect_shared);
            EXPECT_EQ(low_cardinality.isSharedDictionary(), test_case.expect_shared);
            EXPECT_FALSE(low_cardinality.hasSingleDictionaryForPart());
            EXPECT_EQ(low_cardinality.isNullAt(0), test_case.destination_is_nullable);
            if (!test_case.destination_is_nullable)
            {
                EXPECT_EQ(low_cardinality.getFloat64(0), test_case.destination_value);
                EXPECT_FALSE(std::signbit(low_cardinality.getFloat64(0)));
            }
            EXPECT_FALSE(low_cardinality.isNullAt(1));
            EXPECT_EQ(low_cardinality.getFloat64(1), 10);
        }
    };

    check_insertion("insertFrom", [](IColumn & destination, const IColumn & source)
    {
        destination.insertFrom(source, 0);
    });
    check_insertion("insertManyFrom", [](IColumn & destination, const IColumn & source)
    {
        destination.insertManyFrom(source, 0, 1);
    });
    check_insertion("insertRangeFrom", [](IColumn & destination, const IColumn & source)
    {
        destination.insertRangeFrom(source, 0, 1);
    });
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
