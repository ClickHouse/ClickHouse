#include <Server/ArrowFlight/FlightSQLTypeInfo.h>

#include <DataTypes/DataTypeFixedString.h>

#include <arrow/array/builder_binary.h>

#include <array>


namespace DB::ArrowFlight
{
namespace
{

/// ODBC SQL type codes used by `CommandGetXdbcTypeInfo`.
constexpr int32_t SQL_GUID = -11;
constexpr int32_t SQL_BIT = -7;
constexpr int32_t SQL_TINYINT = -6;
constexpr int32_t SQL_BIGINT = -5;
constexpr int32_t SQL_BINARY = -2;
constexpr int32_t SQL_DECIMAL = 3;
constexpr int32_t SQL_INTEGER = 4;
constexpr int32_t SQL_SMALLINT = 5;
constexpr int32_t SQL_REAL = 7;
constexpr int32_t SQL_DOUBLE = 8;
constexpr int32_t SQL_VARCHAR = 12;
constexpr int32_t SQL_TYPE_DATE = 91;

constexpr int32_t SQL_CODE_DATE = 1;

constexpr int32_t SQL_SEARCHABLE_BASIC = 2;
constexpr int32_t SQL_SEARCHABLE_FULL = 3;

/// Ordered by `data_type` ascending, then `type_name` (protocol requirement).
/// `Enum8` and `Enum16` are omitted because Arrow exports their numeric codes
/// without a standard mapping back to the ClickHouse labels. Wide integers are
/// omitted because Arrow exports their in-memory bytes as fixed-size binary.
/// `DateTime` is omitted because Arrow exports it as `uint32`, even with an explicit time zone.
/// `DateTime64` uses Arrow `timestamp`, but without an explicit time zone its schema can depend on
/// `session_timezone`. Both families stay outside this catalog.
/// `String` retains its existing Arrow `Utf8` representation, which requires valid UTF-8 values.
constexpr std::array<XdbcTypeInfoRow, 17> type_info_rows = {{
    {.type_name = "UUID",
     .data_type = SQL_GUID,
     .column_size = 36,
     .literal_prefix = "'",
     .literal_suffix = "'",
     .searchable = SQL_SEARCHABLE_BASIC},
    {.type_name = "Bool", .data_type = SQL_BIT, .column_size = 1, .searchable = SQL_SEARCHABLE_BASIC},
    {.type_name = "Int8",
     .data_type = SQL_TINYINT,
     .column_size = 3,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .num_prec_radix = 10},
    {.type_name = "UInt8",
     .data_type = SQL_TINYINT,
     .column_size = 3,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .unsigned_attribute = true,
     .num_prec_radix = 10},
    {.type_name = "Int64",
     .data_type = SQL_BIGINT,
     .column_size = 19,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .num_prec_radix = 10},
    {.type_name = "UInt64",
     .data_type = SQL_BIGINT,
     .column_size = 20,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .unsigned_attribute = true,
     .num_prec_radix = 10},
    {.type_name = "FixedString",
     .data_type = SQL_BINARY,
     .column_size = MAX_FIXEDSTRING_SIZE,
     .literal_prefix = "'",
     .literal_suffix = "'",
     .create_params = "length",
     .case_sensitive = true,
     .searchable = SQL_SEARCHABLE_FULL},
    {.type_name = "Decimal",
     .data_type = SQL_DECIMAL,
     .column_size = 76,
     .create_params = "precision,scale",
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .minimum_scale = 0,
     .maximum_scale = 76,
     .num_prec_radix = 10},
    {.type_name = "Int32",
     .data_type = SQL_INTEGER,
     .column_size = 10,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .num_prec_radix = 10},
    {.type_name = "UInt32",
     .data_type = SQL_INTEGER,
     .column_size = 10,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .unsigned_attribute = true,
     .num_prec_radix = 10},
    {.type_name = "Int16",
     .data_type = SQL_SMALLINT,
     .column_size = 5,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .num_prec_radix = 10},
    {.type_name = "UInt16",
     .data_type = SQL_SMALLINT,
     .column_size = 5,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .unsigned_attribute = true,
     .num_prec_radix = 10},
    {.type_name = "Float32",
     .data_type = SQL_REAL,
     .column_size = 24,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .num_prec_radix = 2},
    {.type_name = "Float64",
     .data_type = SQL_DOUBLE,
     .column_size = 53,
     .searchable = SQL_SEARCHABLE_BASIC,
     .numeric = true,
     .num_prec_radix = 2},
    {.type_name = "String",
     .data_type = SQL_VARCHAR,
     .column_size = arrow::StringBuilder::memory_limit(),
     .literal_prefix = "'",
     .literal_suffix = "'",
     .case_sensitive = true,
     .searchable = SQL_SEARCHABLE_FULL},
    {.type_name = "Date",
     .data_type = SQL_TYPE_DATE,
     .column_size = 10,
     .literal_prefix = "'",
     .literal_suffix = "'",
     .searchable = SQL_SEARCHABLE_BASIC,
     .datetime_subcode = SQL_CODE_DATE},
    {.type_name = "Date32",
     .data_type = SQL_TYPE_DATE,
     .column_size = 10,
     .literal_prefix = "'",
     .literal_suffix = "'",
     .searchable = SQL_SEARCHABLE_BASIC,
     .datetime_subcode = SQL_CODE_DATE},
}};

}

std::span<const XdbcTypeInfoRow> getXdbcTypeInfoRows()
{
    return type_info_rows;
}

}
