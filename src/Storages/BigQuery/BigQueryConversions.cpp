#include <Storages/BigQuery/BigQueryConversions.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <cmath>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

const FormatSettings & formatSettings()
{
    static const FormatSettings settings;
    return settings;
}

[[noreturn]] void throwIncorrectValue(const BigQueryField & field, const String & value)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected value '{}' of BigQuery field '{}'", value, field.name);
}

/// Parse the whole string as a number, throw if any characters remain.
template <typename T>
T parseStrict(const String & text, const BigQueryField & field)
{
    ReadBufferFromString buf(text);
    T result;
    if (!tryReadText(result, buf) || !buf.eof())
        throwIncorrectValue(field, text);
    return result;
}

void insertLeafValue(IColumn & column, const DataTypePtr & type, const BigQueryField & field, const Poco::Dynamic::Var & value);

/// Inserts a value that is not wrapped in Array (either a scalar/record or one element of a REPEATED field).
void insertNonRepeatedValue(IColumn & column, const DataTypePtr & type, const BigQueryField & field, const Poco::Dynamic::Var & value)
{
    if (auto * column_nullable = typeid_cast<ColumnNullable *>(&column))
    {
        if (value.isEmpty())
        {
            column_nullable->insertDefault();
            return;
        }
        const auto & nested_type = assert_cast<const DataTypeNullable &>(*type).getNestedType();
        insertLeafValue(column_nullable->getNestedColumn(), nested_type, field, value);
        column_nullable->getNullMapData().push_back(false);
        return;
    }

    if (value.isEmpty())
    {
        /// A NULL for a type that cannot be inside Nullable (e.g. a NULLABLE RECORD): insert default values.
        column.insertDefault();
        return;
    }

    insertLeafValue(column, type, field, value);
}

void insertLeafValue(IColumn & column, const DataTypePtr & type, const BigQueryField & field, const Poco::Dynamic::Var & value)
{
    if (field.type == BigQueryField::Type::Record)
    {
        /// {"v": {"f": [{"v": ...}, ...]}} with cells in schema order.
        Poco::JSON::Object::Ptr record;
        if (value.type() == typeid(Poco::JSON::Object::Ptr))
            record = value.extract<Poco::JSON::Object::Ptr>();
        if (!record || !record->isArray("f"))
            throwIncorrectValue(field, value.toString());

        auto cells = record->getArray("f");
        if (cells->size() != field.children.size())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "BigQuery RECORD field '{}' has {} values, expected {}",
                field.name, cells->size(), field.children.size());

        auto & column_tuple = assert_cast<ColumnTuple &>(column);
        const auto & tuple_types = assert_cast<const DataTypeTuple &>(*type).getElements();
        for (size_t i = 0; i < field.children.size(); ++i)
        {
            auto cell = cells->getObject(static_cast<unsigned>(i));
            if (!cell)
                throwIncorrectValue(field, value.toString());
            insertBigQueryValue(column_tuple.getColumn(i), tuple_types[i], field.children[i], cell->get("v"));
        }
        return;
    }

    const String text = value.convert<String>();

    switch (field.type)
    {
        case BigQueryField::Type::String:
        case BigQueryField::Type::Geography:
        case BigQueryField::Type::JSON:
        case BigQueryField::Type::Interval:
        case BigQueryField::Type::Range:
        {
            assert_cast<ColumnString &>(column).insertData(text.data(), text.size());
            return;
        }
        case BigQueryField::Type::Bytes:
        {
            const String decoded = base64Decode(text);
            assert_cast<ColumnString &>(column).insertData(decoded.data(), decoded.size());
            return;
        }
        case BigQueryField::Type::Integer:
        {
            assert_cast<ColumnInt64 &>(column).insertValue(parseStrict<Int64>(text, field));
            return;
        }
        case BigQueryField::Type::Float:
        {
            Float64 float_value = 0;
            if (text == "Infinity")
                float_value = std::numeric_limits<Float64>::infinity();
            else if (text == "-Infinity")
                float_value = -std::numeric_limits<Float64>::infinity();
            else if (text == "NaN")
                float_value = std::numeric_limits<Float64>::quiet_NaN();
            else
                float_value = parseStrict<Float64>(text, field);
            assert_cast<ColumnFloat64 &>(column).insertValue(float_value);
            return;
        }
        case BigQueryField::Type::Boolean:
        {
            if (text != "true" && text != "false")
                throwIncorrectValue(field, text);
            assert_cast<ColumnUInt8 &>(column).insertValue(text == "true");
            return;
        }
        case BigQueryField::Type::Timestamp:
        {
            /// Microseconds since the epoch (formatOptions.useInt64Timestamp=true).
            assert_cast<ColumnDateTime64 &>(column).insertValue(DateTime64(parseStrict<Int64>(text, field)));
            return;
        }
        case BigQueryField::Type::Date:
        case BigQueryField::Type::Time:
        case BigQueryField::Type::DateTime:
        case BigQueryField::Type::Numeric:
        case BigQueryField::Type::BigNumeric:
        {
            /// The BigQuery text formats of these types match the ClickHouse ones
            /// ('T' as the date-time separator is accepted).
            ReadBufferFromString buf(text);
            try
            {
                type->getDefaultSerialization()->deserializeWholeText(column, buf, formatSettings());
            }
            catch (Exception & e)
            {
                e.addMessage("while reading value '{}' of BigQuery field '{}'", text, field.name);
                throw;
            }
            return;
        }
        case BigQueryField::Type::Record:
            break;
    }

    throwIncorrectValue(field, text);
}

}

void insertBigQueryValue(IColumn & column, const DataTypePtr & type, const BigQueryField & field, const Poco::Dynamic::Var & value)
{
    if (field.repeated)
    {
        auto & column_array = assert_cast<ColumnArray &>(column);
        auto & offsets = column_array.getOffsets();

        /// A NULL array and an empty array are indistinguishable in BigQuery, both come as null.
        if (value.isEmpty())
        {
            offsets.push_back(offsets.back());
            return;
        }

        Poco::JSON::Array::Ptr elements;
        if (value.type() == typeid(Poco::JSON::Array::Ptr))
            elements = value.extract<Poco::JSON::Array::Ptr>();
        if (!elements)
            throwIncorrectValue(field, value.toString());

        const auto & nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
        for (size_t i = 0; i < elements->size(); ++i)
        {
            /// Each element is wrapped as {"v": ...}.
            auto element = elements->getObject(static_cast<unsigned>(i));
            if (!element)
                throwIncorrectValue(field, value.toString());
            insertNonRepeatedValue(column_array.getData(), nested_type, field, element->get("v"));
        }
        offsets.push_back(offsets.back() + elements->size());
        return;
    }

    insertNonRepeatedValue(column, type, field, value);
}

namespace
{

Poco::Dynamic::Var leafJSONValue(const BigQueryField & field, const DataTypePtr & type, const IColumn & column, size_t row)
{
    switch (field.type)
    {
        case BigQueryField::Type::String:
        case BigQueryField::Type::Geography:
        case BigQueryField::Type::JSON:
        case BigQueryField::Type::Interval:
        {
            return Poco::Dynamic::Var(String(assert_cast<const ColumnString &>(column).getDataAt(row)));
        }
        case BigQueryField::Type::Range:
        {
            /// RANGE columns are read-only: `tabledata.insertAll` expects a structured
            /// `{start, end}` object, which cannot be reconstructed from the opaque String
            /// mapping. Writes are rejected earlier in `StorageBigQuery::write`; this is a
            /// defensive guard in case a value ever reaches this point.
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Writing to a BigQuery RANGE column ('{}') is not supported; RANGE columns are read-only", field.name);
        }
        case BigQueryField::Type::Bytes:
        {
            return Poco::Dynamic::Var(base64Encode(String(assert_cast<const ColumnString &>(column).getDataAt(row))));
        }
        case BigQueryField::Type::Integer:
        {
            /// `tabledata.insertAll` parses JSON numbers as IEEE-754 doubles, which lose precision for
            /// INT64 values outside [-2^53 + 1, 2^53 - 1]. Serialize the value as decimal text (BigQuery
            /// accepts INT64 as a string) so that a large `Int64` is stored exactly rather than corrupted.
            return Poco::Dynamic::Var(std::to_string(assert_cast<const ColumnInt64 &>(column).getElement(row)));
        }
        case BigQueryField::Type::Float:
        {
            Float64 float_value = assert_cast<const ColumnFloat64 &>(column).getElement(row);
            /// JSON has no literals for these, but BigQuery accepts them as strings.
            if (std::isnan(float_value))
                return Poco::Dynamic::Var(String("NaN"));
            if (std::isinf(float_value))
                return Poco::Dynamic::Var(String(float_value > 0 ? "Infinity" : "-Infinity"));
            return Poco::Dynamic::Var(float_value);
        }
        case BigQueryField::Type::Boolean:
        {
            return Poco::Dynamic::Var(assert_cast<const ColumnUInt8 &>(column).getElement(row) != 0);
        }
        case BigQueryField::Type::Timestamp:
        case BigQueryField::Type::Date:
        case BigQueryField::Type::Time:
        case BigQueryField::Type::DateTime:
        case BigQueryField::Type::Numeric:
        case BigQueryField::Type::BigNumeric:
        {
            /// The ClickHouse text formats of these types are accepted by BigQuery.
            /// A TIMESTAMP without a time zone suffix is interpreted as UTC, which matches
            /// the DateTime64(6, 'UTC') serialization.
            WriteBufferFromOwnString out;
            type->getDefaultSerialization()->serializeText(column, row, out, formatSettings());
            return Poco::Dynamic::Var(out.str());
        }
        case BigQueryField::Type::Record:
        {
            Poco::JSON::Object::Ptr object = new Poco::JSON::Object;
            const auto & column_tuple = assert_cast<const ColumnTuple &>(column);
            const auto & tuple_types = assert_cast<const DataTypeTuple &>(*type).getElements();
            for (size_t i = 0; i < field.children.size(); ++i)
                object->set(field.children[i].name, bigQueryJSONValue(field.children[i], tuple_types[i], column_tuple.getColumn(i), row));
            return Poco::Dynamic::Var(object);
        }
    }
}

Poco::Dynamic::Var nonRepeatedJSONValue(const BigQueryField & field, const DataTypePtr & type, const IColumn & column, size_t row)
{
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (column_nullable->isNullAt(row))
            return {};
        const auto & nested_type = assert_cast<const DataTypeNullable &>(*type).getNestedType();
        return leafJSONValue(field, nested_type, column_nullable->getNestedColumn(), row);
    }
    return leafJSONValue(field, type, column, row);
}

}

Poco::Dynamic::Var bigQueryJSONValue(const BigQueryField & field, const DataTypePtr & type, const IColumn & column, size_t row)
{
    if (field.repeated)
    {
        const auto & column_array = assert_cast<const ColumnArray &>(column);
        const auto & offsets = column_array.getOffsets();
        const auto & nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();

        Poco::JSON::Array::Ptr elements = new Poco::JSON::Array;
        size_t start = row == 0 ? 0 : offsets[row - 1];
        for (size_t i = start; i < offsets[row]; ++i)
            elements->add(nonRepeatedJSONValue(field, nested_type, column_array.getData(), i));
        return Poco::Dynamic::Var(elements);
    }

    return nonRepeatedJSONValue(field, type, column, row);
}

}
