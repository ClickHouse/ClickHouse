#include <Storages/BigQuery/BigQuerySchema.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>

#include <Poco/JSON/Array.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// BigQuery TIMESTAMP and DATETIME have microsecond precision.
constexpr UInt32 BIGQUERY_SUBSECOND_SCALE = 6;

/// Defaults from https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
constexpr UInt64 BIGQUERY_NUMERIC_PRECISION = 38;
constexpr UInt64 BIGQUERY_NUMERIC_SCALE = 9;
constexpr UInt64 BIGQUERY_BIGNUMERIC_PRECISION = 76;
constexpr UInt64 BIGQUERY_BIGNUMERIC_SCALE = 38;

BigQueryField::Type parseFieldType(const String & type_name)
{
    /// tables.get returns legacy type names, but accept standard SQL aliases too.
    if (type_name == "STRING")
        return BigQueryField::Type::String;
    if (type_name == "BYTES")
        return BigQueryField::Type::Bytes;
    if (type_name == "INTEGER" || type_name == "INT64")
        return BigQueryField::Type::Integer;
    if (type_name == "FLOAT" || type_name == "FLOAT64")
        return BigQueryField::Type::Float;
    if (type_name == "BOOLEAN" || type_name == "BOOL")
        return BigQueryField::Type::Boolean;
    if (type_name == "TIMESTAMP")
        return BigQueryField::Type::Timestamp;
    if (type_name == "DATE")
        return BigQueryField::Type::Date;
    if (type_name == "TIME")
        return BigQueryField::Type::Time;
    if (type_name == "DATETIME")
        return BigQueryField::Type::DateTime;
    if (type_name == "NUMERIC" || type_name == "DECIMAL")
        return BigQueryField::Type::Numeric;
    if (type_name == "BIGNUMERIC" || type_name == "BIGDECIMAL")
        return BigQueryField::Type::BigNumeric;
    if (type_name == "GEOGRAPHY")
        return BigQueryField::Type::Geography;
    if (type_name == "JSON")
        return BigQueryField::Type::JSON;
    if (type_name == "INTERVAL")
        return BigQueryField::Type::Interval;
    if (type_name == "RANGE")
        return BigQueryField::Type::Range;
    if (type_name == "RECORD" || type_name == "STRUCT")
        return BigQueryField::Type::Record;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BigQuery type '{}' is not supported", type_name);
}

DataTypePtr computeClickHouseType(const BigQueryField & field)
{
    DataTypePtr base;
    switch (field.type)
    {
        case BigQueryField::Type::String:
        case BigQueryField::Type::Bytes:
        case BigQueryField::Type::Geography:
        case BigQueryField::Type::JSON:
        case BigQueryField::Type::Interval:
        case BigQueryField::Type::Range:
            base = std::make_shared<DataTypeString>();
            break;
        case BigQueryField::Type::Integer:
            base = std::make_shared<DataTypeInt64>();
            break;
        case BigQueryField::Type::Float:
            base = std::make_shared<DataTypeFloat64>();
            break;
        case BigQueryField::Type::Boolean:
            base = DataTypeFactory::instance().get("Bool");
            break;
        case BigQueryField::Type::Timestamp:
            base = std::make_shared<DataTypeDateTime64>(BIGQUERY_SUBSECOND_SCALE, "UTC");
            break;
        case BigQueryField::Type::Date:
            base = std::make_shared<DataTypeDate32>();
            break;
        case BigQueryField::Type::Time:
            base = std::make_shared<DataTypeTime64>(BIGQUERY_SUBSECOND_SCALE);
            break;
        case BigQueryField::Type::DateTime:
            /// DATETIME is a civil time without a time zone. UTC is specified explicitly so that
            /// the digits do not depend on the server time zone.
            base = std::make_shared<DataTypeDateTime64>(BIGQUERY_SUBSECOND_SCALE, "UTC");
            break;
        case BigQueryField::Type::Numeric:
            base = createDecimal<DataTypeDecimal>(
                field.precision ? field.precision : BIGQUERY_NUMERIC_PRECISION,
                field.precision ? field.scale : BIGQUERY_NUMERIC_SCALE);
            break;
        case BigQueryField::Type::BigNumeric:
            base = createDecimal<DataTypeDecimal>(
                field.precision ? field.precision : BIGQUERY_BIGNUMERIC_PRECISION,
                field.precision ? field.scale : BIGQUERY_BIGNUMERIC_SCALE);
            break;
        case BigQueryField::Type::Record:
        {
            DataTypes element_types;
            Names element_names;
            element_types.reserve(field.children.size());
            element_names.reserve(field.children.size());
            for (const auto & child : field.children)
            {
                element_types.push_back(child.data_type);
                element_names.push_back(child.name);
            }
            base = std::make_shared<DataTypeTuple>(element_types, element_names);
            break;
        }
    }

    if (field.repeated)
    {
        /// A BigQuery `ARRAY` (a `REPEATED` field) cannot store `NULL` elements: `ARRAY<T>` is equivalent
        /// to `ARRAY<T NOT NULL>`, so `tabledata.list` never returns a `{"v": null}` element for a stored
        /// table. The element type is therefore not `Nullable` (`Array(T)`, or `Array(Tuple(...))` for a
        /// `RECORD` element), which also keeps `DESCRIBE` / `SHOW CREATE` honest and lets a natural
        /// `Array(T)` be declared explicitly. A `NULL` element in a response is treated as malformed input
        /// on read rather than baked into the schema. A `NULL` array and an empty array are
        /// indistinguishable in BigQuery and both map to an empty ClickHouse array.
        return std::make_shared<DataTypeArray>(base);
    }
    /// A NULLABLE field is mapped to a Nullable type so a remote NULL is preserved instead of
    /// collapsing to a default. A NULLABLE RECORD becomes `Nullable(Tuple(...))`; persisting such a
    /// column with the `BigQuery` engine requires the `enable_nullable_tuple_type` setting, as for
    /// any `Nullable(Tuple)` column.
    if (!field.required && base->canBeInsideNullable())
        return std::make_shared<DataTypeNullable>(base);
    return base;
}

BigQueryField parseField(const Poco::JSON::Object::Ptr & field_object)
{
    BigQueryField field;

    if (!field_object->has("name") || !field_object->has("type"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "BigQuery schema field must have 'name' and 'type'");

    field.name = field_object->getValue<String>("name");
    field.type = parseFieldType(field_object->getValue<String>("type"));

    String mode = "NULLABLE";
    if (field_object->has("mode"))
        mode = field_object->getValue<String>("mode");
    field.repeated = mode == "REPEATED";
    field.required = mode == "REQUIRED";

    /// tables.get serializes int64 values as JSON strings.
    if (field_object->has("precision"))
        field.precision = field_object->get("precision").convert<UInt64>();
    if (field_object->has("scale"))
        field.scale = field_object->get("scale").convert<UInt64>();

    if (field.type == BigQueryField::Type::Record)
    {
        auto children = field_object->getArray("fields");
        if (!children || children->size() == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "BigQuery RECORD field '{}' has no nested fields", field.name);
        for (size_t i = 0; i < children->size(); ++i)
        {
            auto child = children->getObject(static_cast<unsigned>(i));
            if (!child)
                throw Exception(ErrorCodes::INCORRECT_DATA, "BigQuery schema of '{}' has a malformed nested field", field.name);
            field.children.push_back(parseField(child));
        }
    }

    field.data_type = computeClickHouseType(field);
    return field;
}

}

BigQueryFields parseBigQueryTableSchema(const Poco::JSON::Object::Ptr & table_object)
{
    auto schema = table_object->getObject("schema");
    if (!schema || !schema->has("fields"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "BigQuery table metadata has no schema");

    auto fields_array = schema->getArray("fields");
    if (!fields_array || fields_array->size() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "BigQuery table schema has no fields");

    BigQueryFields fields;
    fields.reserve(fields_array->size());
    for (size_t i = 0; i < fields_array->size(); ++i)
    {
        auto field_object = fields_array->getObject(static_cast<unsigned>(i));
        if (!field_object)
            throw Exception(ErrorCodes::INCORRECT_DATA, "BigQuery table schema has a malformed field");
        fields.push_back(parseField(field_object));
    }
    return fields;
}

ColumnsDescription columnsDescriptionFromBigQuerySchema(const BigQueryFields & fields)
{
    NamesAndTypesList names_and_types;
    for (const auto & field : fields)
        names_and_types.emplace_back(field.name, field.data_type);
    return ColumnsDescription{names_and_types};
}

const BigQueryField * findBigQueryField(const BigQueryFields & fields, const String & name)
{
    for (const auto & field : fields)
        if (field.name == name)
            return &field;
    return nullptr;
}

}
