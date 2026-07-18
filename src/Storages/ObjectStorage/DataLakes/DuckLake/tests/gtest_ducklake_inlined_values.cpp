#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeInlinedValues.h>

#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/TimezoneMixin.h>

using namespace DB;
using namespace DB::DuckLake;

namespace
{

template <typename T>
void expectScalar(const String & value, const DataTypePtr & type, const T & expected, bool postgres = false)
{
    const Field parsed = parseInlinedValue(value, type, postgres);
    EXPECT_EQ(parsed.safeGet<T>(), expected) << "value: " << value;
}

}

TEST(DuckLakeInlinedValues, Scalars)
{
    expectScalar("-42", std::make_shared<DataTypeInt32>(), Int32(-42));
    expectScalar("12345", std::make_shared<DataTypeInt128>(), Int128(12345));
    expectScalar("18446744073709551615", std::make_shared<DataTypeUInt64>(), UInt64(18446744073709551615ULL));
    expectScalar("1.5", std::make_shared<DataTypeFloat64>(), Float64(1.5));
    /// Postgres float text output
    expectScalar("Infinity", std::make_shared<DataTypeFloat64>(), Float64(std::numeric_limits<Float64>::infinity()), true);
    expectScalar("-Infinity", std::make_shared<DataTypeFloat32>(), Float64(-std::numeric_limits<Float64>::infinity()), true);
    EXPECT_TRUE(std::isnan(parseInlinedValue("NaN", std::make_shared<DataTypeFloat64>(), true).safeGet<Float64>()));

    /// Booleans: sqlite stores 0/1, postgres t/f, nested values true/false
    expectScalar("1", std::make_shared<DataTypeUInt8>(), UInt64(1));
    expectScalar("t", std::make_shared<DataTypeUInt8>(), UInt64(1), true);
    expectScalar("f", std::make_shared<DataTypeUInt8>(), UInt64(0), true);

    EXPECT_EQ(parseInlinedValue("12.34", std::make_shared<DataTypeDecimal<Decimal64>>(10, 2), false).safeGet<DecimalField<Decimal64>>().getValue(), Decimal64(1234));

    const Field date = parseInlinedValue("2024-01-15", std::make_shared<DataTypeDate32>(), false);
    EXPECT_EQ(date.safeGet<Int32>(), 19737); /// days since epoch

    const Field ts = parseInlinedValue("2024-01-15 10:30:00.123456", std::make_shared<DataTypeDateTime64>(6), false);
    EXPECT_EQ(ts.safeGet<DecimalField<DateTime64>>().getValue().value, 1705314600123456LL);

    const Field uuid = parseInlinedValue(
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", std::make_shared<DataTypeUUID>(), false);
    EXPECT_EQ(applyVisitor(FieldVisitorToString(), uuid), "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
}

TEST(DuckLakeInlinedValues, TimestampWithTimezoneOffset)
{
    const auto type = std::make_shared<DataTypeDateTime64>(6, TimezoneMixin{"UTC"});

    /// +00 keeps the instant
    Field parsed = parseInlinedValue("2024-01-15 10:30:00+00", type, false);
    EXPECT_EQ(parsed.safeGet<DecimalField<DateTime64>>().getValue().value, 1705314600000000LL);

    /// a positive offset means local time ahead of UTC: subtract it
    parsed = parseInlinedValue("2024-01-15 12:30:00+02", type, false);
    EXPECT_EQ(parsed.safeGet<DecimalField<DateTime64>>().getValue().value, 1705314600000000LL);

    parsed = parseInlinedValue("2024-01-15 09:00:00-01:30", type, false);
    EXPECT_EQ(parsed.safeGet<DecimalField<DateTime64>>().getValue().value, 1705314600000000LL);

    /// no offset: parsed as-is
    parsed = parseInlinedValue("2024-01-15 10:30:00", type, false);
    EXPECT_EQ(parsed.safeGet<DecimalField<DateTime64>>().getValue().value, 1705314600000000LL);
}

TEST(DuckLakeInlinedValues, Strings)
{
    const auto type = std::make_shared<DataTypeString>();
    EXPECT_EQ(parseInlinedValue("hello", type, false).safeGet<String>(), "hello");
    EXPECT_EQ(parseInlinedValue("weird 'quote", type, false).safeGet<String>(), "weird 'quote");

    /// postgres bytea hex
    EXPECT_EQ(parseInlinedValue("\\x68656c6c6f", type, true).safeGet<String>(), "hello");
    EXPECT_EQ(parseInlinedValue("\\x00ff", type, true).safeGet<String>(), String("\x00\xff", 2));
}

TEST(DuckLakeInlinedValues, Nested)
{
    const auto struct_type = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        Strings{"x", "y"});

    Field parsed = parseInlinedValue("{'x': 1, 'y': 'u'}", struct_type, false);
    const Tuple & tuple = parsed.safeGet<Tuple>();
    EXPECT_EQ(tuple[0].safeGet<Int32>(), 1);
    EXPECT_EQ(tuple[1].safeGet<String>(), "u");

    /// quoted string with escaped quote and a space
    parsed = parseInlinedValue("{'y': 'v ''w'' x', 'x': NULL}", struct_type, false);
    const Tuple & tuple2 = parsed.safeGet<Tuple>();
    EXPECT_TRUE(tuple2[0].isNull());
    EXPECT_EQ(tuple2[1].safeGet<String>(), "v 'w' x");

    const auto list_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()));
    parsed = parseInlinedValue("[1, 2, NULL]", list_type, false);
    const Array & array = parsed.safeGet<Array>();
    EXPECT_EQ(array.size(), 3);
    EXPECT_EQ(array[0].safeGet<Int32>(), 1);
    EXPECT_TRUE(array[2].isNull());

    /// bare string elements (DuckDB list display format)
    const auto string_list = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    parsed = parseInlinedValue("[a, b c, 'd e']", string_list, false);
    const Array & strings = parsed.safeGet<Array>();
    EXPECT_EQ(strings[0].safeGet<String>(), "a");
    EXPECT_EQ(strings[1].safeGet<String>(), "b c");
    EXPECT_EQ(strings[2].safeGet<String>(), "d e");

    const auto map_type = std::make_shared<DataTypeMap>(
        std::make_shared<DataTypeString>(),
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()));
    parsed = parseInlinedValue("{a=1, b=2}", map_type, false);
    const Map & map = parsed.safeGet<Map>();
    EXPECT_EQ(map.size(), 2);
    EXPECT_EQ(map[0].safeGet<Tuple>()[0].safeGet<String>(), "a");
    EXPECT_EQ(map[0].safeGet<Tuple>()[1].safeGet<Int32>(), 1);
    EXPECT_EQ(map[1].safeGet<Tuple>()[0].safeGet<String>(), "b");

    /// cast suffixes on nested scalars are skipped
    parsed = parseInlinedValue("['2024-01-15'::date, 'x'::varchar]", string_list, false);
    EXPECT_EQ(parsed.safeGet<Array>()[0].safeGet<String>(), "2024-01-15");

    /// empty containers
    parsed = parseInlinedValue("[]", list_type, false);
    EXPECT_TRUE(parsed.safeGet<Array>().empty());
    parsed = parseInlinedValue("{}", map_type, false);
    EXPECT_TRUE(parsed.safeGet<Map>().empty());

    EXPECT_THROW(parseInlinedValue("{'x': }", struct_type, false), Exception);
    EXPECT_THROW(parseInlinedValue("[1, 2", list_type, false), Exception);
}

TEST(DuckLakeInlinedValues, Columns)
{
    const auto nullable_int = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    std::vector<std::optional<String>> values = {"1", std::nullopt, "3"};
    ColumnPtr column = buildInlinedColumn(values, nullable_int, false);
    EXPECT_EQ(column->size(), 3);
    EXPECT_EQ((*column)[0].safeGet<Int32>(), 1);
    EXPECT_TRUE((*column)[1].isNull());
    EXPECT_EQ((*column)[2].safeGet<Int32>(), 3);

    /// NULL in a non-nullable scalar throws
    EXPECT_THROW(buildInlinedColumn(values, std::make_shared<DataTypeInt32>(), false), Exception);

    /// NULL in a struct column becomes the default tuple (like in the Parquet reader)
    const auto struct_type = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        Strings{"x", "y"});
    std::vector<std::optional<String>> nested_values = {std::nullopt, "{'x': 1, 'y': 'u'}"};
    column = buildInlinedColumn(nested_values, struct_type, false);
    EXPECT_EQ(column->size(), 2);
    EXPECT_TRUE((*column)[0].safeGet<Tuple>()[0].isNull());
    EXPECT_EQ((*column)[1].safeGet<Tuple>()[1].safeGet<String>(), "u");
}
