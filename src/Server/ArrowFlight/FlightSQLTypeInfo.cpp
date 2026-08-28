#include <Server/ArrowFlight/FlightSQLTypeInfo.h>

#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/column_metadata.h>

#include <array>
#include <string>


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
constexpr int32_t SQL_TYPE_TIMESTAMP = 93;

constexpr int32_t SQL_CODE_DATE = 1;
constexpr int32_t SQL_CODE_TIMESTAMP = 3;

constexpr int32_t SQL_SEARCHABLE_BASIC = 2;
constexpr int32_t SQL_SEARCHABLE_FULL = 3;

/// Ordered by `data_type` ascending, then `type_name` (protocol requirement).
/// `Enum8` and `Enum16` are omitted because Arrow exports their numeric codes
/// without a standard mapping back to the ClickHouse labels. Wide integers are
/// omitted because Arrow exports their in-memory bytes as fixed-size binary.
constexpr std::array<XdbcTypeInfoRow, 19> type_info_rows = {{
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
    {.type_name = "DateTime",
     .data_type = SQL_TYPE_TIMESTAMP,
     .column_size = 19,
     .literal_prefix = "'",
     .literal_suffix = "'",
     .create_params = "timezone",
     .searchable = SQL_SEARCHABLE_BASIC,
     .datetime_subcode = SQL_CODE_TIMESTAMP,
     .minimum_scale = 0,
     .maximum_scale = 0},
    {.type_name = "DateTime64",
     .data_type = SQL_TYPE_TIMESTAMP,
     .column_size = 29,
     .literal_prefix = "'",
     .literal_suffix = "'",
     .create_params = "precision,timezone",
     .searchable = SQL_SEARCHABLE_BASIC,
     .datetime_subcode = SQL_CODE_TIMESTAMP,
     .minimum_scale = 0,
     .maximum_scale = 9},
}};

DataTypePtr unwrapType(const DataTypePtr & type)
{
    auto result = type;
    while (result->isNullable() || result->lowCardinality())
    {
        if (result->isNullable())
            result = removeNullable(result);
        else
            result = removeLowCardinality(result);
    }
    return result;
}

bool usesRegisteredColumnSizeAsPrecision(std::string_view family_name, const XdbcTypeInfoRow & row)
{
    return row.numeric || family_name == "Bool" || family_name == "UUID" || family_name == "Date" || family_name == "Date32";
}

std::string getTypeFamilyName(const DataTypePtr & type)
{
    if (const auto * simple_aggregate_function = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
    {
        return getTypeFamilyName(unwrapType(simple_aggregate_function->getArgumentsDataTypes()[0]));
    }

    /// Custom types such as `Bool` and `IPv4` share an underlying family with another
    /// ClickHouse type. Keep the custom identity so unsupported custom types do not
    /// claim the XDBC row of their underlying representation.
    if (type->hasCustomName())
        return type->getName();
    return type->getFamilyName();
}

}

std::span<const XdbcTypeInfoRow> getXdbcTypeInfoRows()
{
    return type_info_rows;
}

const XdbcTypeInfoRow * findXdbcTypeInfo(std::string_view type_name)
{
    const XdbcTypeInfoRow * result = nullptr;
    for (const auto & row : type_info_rows)
    {
        if (row.type_name == type_name)
        {
            if (result)
                return nullptr;
            result = &row;
        }
    }
    return result;
}

arrow::Result<std::shared_ptr<arrow::Schema>>
addFlightSQLTypeMetadata(std::shared_ptr<arrow::Schema> schema, const ColumnsWithTypeAndName & header)
{
    if (schema->num_fields() != static_cast<int>(header.size()))
    {
        return arrow::Status::Invalid(
            "Cannot add Flight SQL type metadata: Arrow schema has ",
            schema->num_fields(),
            " fields, but ClickHouse header has ",
            header.size(),
            " columns");
    }

    arrow::FieldVector fields;
    fields.reserve(header.size());

    for (size_t i = 0; i < header.size(); ++i)
    {
        const auto & column = header[i];
        const auto type = unwrapType(column.type);
        const std::string family_name = getTypeFamilyName(type);
        const auto * type_info = findXdbcTypeInfo(family_name);

        auto metadata_builder = arrow::flight::sql::ColumnMetadata::Builder();
        if (type_info)
            metadata_builder.TypeName(std::string(family_name));

        if (family_name == "Decimal")
        {
            metadata_builder.Precision(static_cast<int32_t>(getDecimalPrecision(*type)));
            metadata_builder.Scale(static_cast<int32_t>(getDecimalScale(*type)));
        }
        else if (family_name == "DateTime")
        {
            metadata_builder.Precision(19);
            metadata_builder.Scale(0);
        }
        else if (family_name == "DateTime64")
        {
            const auto scale = static_cast<int32_t>(assert_cast<const DataTypeDateTime64 &>(*type).getScale());
            metadata_builder.Precision(19 + (scale == 0 ? 0 : scale + 1));
            metadata_builder.Scale(scale);
        }
        else if (family_name == "FixedString")
        {
            metadata_builder.Precision(static_cast<int32_t>(assert_cast<const DataTypeFixedString &>(*type).getN()));
        }
        else if (type_info && usesRegisteredColumnSizeAsPrecision(family_name, *type_info))
        {
            metadata_builder.Precision(type_info->column_size);
        }

        auto metadata = metadata_builder.Build().metadata_map()->Copy();
        metadata->Append("CLICKHOUSE:TYPE_NAME", column.type->getName());
        fields.emplace_back(schema->field(static_cast<int>(i))->WithMergedMetadata(metadata));
    }

    return std::make_shared<arrow::Schema>(std::move(fields), schema->metadata());
}

}
