#include "config.h"

#if USE_LIBPQXX

#include <gtest/gtest.h>

#include <Core/PostgreSQL/insertPostgreSQLValue.h>
#include <Core/ExternalResultDescription.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>


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

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
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

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
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

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
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

#endif
