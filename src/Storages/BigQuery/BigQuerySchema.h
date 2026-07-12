#pragma once

#include <DataTypes/IDataType.h>
#include <Storages/ColumnsDescription.h>

#include <Poco/JSON/Object.h>

namespace DB
{

/// A field of a BigQuery table schema (an entry of `schema.fields` in a `tables.get` response).
struct BigQueryField
{
    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema
    enum class Type
    {
        String,
        Bytes,
        Integer,
        Float,
        Boolean,
        Timestamp,
        Date,
        Time,
        DateTime,
        Numeric,
        BigNumeric,
        Geography,
        JSON,
        Interval,
        Range,
        Record,
    };

    String name;
    Type type = Type::String;
    bool repeated = false;
    bool required = false;
    /// For parameterized Numeric/BigNumeric; 0 means the default precision and scale.
    UInt64 precision = 0;
    UInt64 scale = 0;
    /// For Record.
    std::vector<BigQueryField> children;

    /// The ClickHouse type this field maps to, including Array/Nullable wrappers.
    DataTypePtr data_type;
};

using BigQueryFields = std::vector<BigQueryField>;

/// Parse the `schema` of a BigQuery `tables.get` response and compute ClickHouse types.
BigQueryFields parseBigQueryTableSchema(const Poco::JSON::Object::Ptr & table_object);

ColumnsDescription columnsDescriptionFromBigQuerySchema(const BigQueryFields & fields);

const BigQueryField * findBigQueryField(const BigQueryFields & fields, const String & name);

}
