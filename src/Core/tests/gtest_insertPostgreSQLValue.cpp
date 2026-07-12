#include "config.h"

#if USE_LIBPQXX

#include <gtest/gtest.h>

#include <Core/PostgreSQL/insertPostgreSQLValue.h>
#include <Core/ExternalResultDescription.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>


using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Regression test for dimension underflow in PostgreSQL array parser.
/// When pqxx::array_parser emits row_end before any row_start (e.g. malformed
/// input starting with '}'), the dimension counter must not underflow from 0.
/// See https://github.com/ClickHouse/clickhouse-core-incidents/issues/1693

TEST(InsertPostgreSQLValue, MalformedArrayClosingBracketThrows)
{
    auto nested_type = std::make_shared<DataTypeInt32>();
    auto array_type = std::make_shared<DataTypeArray>(nested_type);
    auto column = ColumnArray::create(ColumnInt32::create());

    UnorderedMapWithMemoryTracking<size_t, PostgreSQLArrayInfo> array_info;
    preparePostgreSQLArrayInfo(array_info, 0, array_type);

    /// Input "}" causes row_end at dimension 0 — must throw BAD_ARGUMENTS,
    /// not underflow size_t to SIZE_MAX and crash.
    try
    {
        insertPostgreSQLValue(
            *column, "}",
            ExternalResultDescription::ValueType::vtArray,
            array_type, array_info, 0);
        FAIL() << "Expected BAD_ARGUMENTS exception for malformed array '}'";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST(InsertPostgreSQLValue, MalformedArrayClosingThenOpeningThrows)
{
    auto nested_type = std::make_shared<DataTypeInt32>();
    auto array_type = std::make_shared<DataTypeArray>(nested_type);
    auto column = ColumnArray::create(ColumnInt32::create());

    UnorderedMapWithMemoryTracking<size_t, PostgreSQLArrayInfo> array_info;
    preparePostgreSQLArrayInfo(array_info, 0, array_type);

    /// Input "}{" also starts with row_end at dimension 0.
    try
    {
        insertPostgreSQLValue(
            *column, "}{",
            ExternalResultDescription::ValueType::vtArray,
            array_type, array_info, 0);
        FAIL() << "Expected BAD_ARGUMENTS exception for malformed array '}{'" ;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST(InsertPostgreSQLValue, WellFormedArraySucceeds)
{
    auto nested_type = std::make_shared<DataTypeInt32>();
    auto array_type = std::make_shared<DataTypeArray>(nested_type);
    auto column = ColumnArray::create(ColumnInt32::create());

    UnorderedMapWithMemoryTracking<size_t, PostgreSQLArrayInfo> array_info;
    preparePostgreSQLArrayInfo(array_info, 0, array_type);

    /// Well-formed "{1,2,3}" must succeed without exceptions.
    EXPECT_NO_THROW(
        insertPostgreSQLValue(
            *column, "{1,2,3}",
            ExternalResultDescription::ValueType::vtArray,
            array_type, array_info, 0));

    /// Verify the column now has one row with 3 elements.
    ASSERT_EQ(column->size(), 1u);
}

/// Regression test for reading a PostgreSQL `uuid` value into a `UUID2` column (which happens when a bare
/// `UUID` column is materialized to `UUID2` under `uuid_type_version = 2`). `UUID2` stores the canonical
/// (big-endian) byte layout, while the `UUID` type stores the half-swapped layout; the reader must swap the
/// halves so that the textual value round-trips. See https://github.com/ClickHouse/ClickHouse/pull/110084

TEST(InsertPostgreSQLValue, UUID2PreservesTextualValue)
{
    auto uuid2_type = DataTypeFactory::instance().get("UUID2");
    auto column = uuid2_type->createColumn();

    const std::string text = "61f0c404-5cb3-11e7-907b-a6006ad3dba0";
    UnorderedMapWithMemoryTracking<size_t, PostgreSQLArrayInfo> array_info;
    insertPostgreSQLValue(
        *column, text,
        ExternalResultDescription::ValueType::vtUUID2,
        uuid2_type, array_info, 0);

    ASSERT_EQ(column->size(), 1u);

    /// Serializing the stored value back must yield the original textual value: this proves it was stored in
    /// the canonical `UUID2` layout (via `swapHalves`) rather than the half-swapped `UUID` layout.
    WriteBufferFromOwnString out;
    uuid2_type->getDefaultSerialization()->serializeText(*column, 0, out, FormatSettings{});
    EXPECT_EQ(out.str(), text);
}

#endif
