#include <Storages/BigQuery/BigQueryConversions.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Formats/FormatSettings.h>
#include <Functions/geometryConverters.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <cmath>
#include <sstream>

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

/// BigQuery returns a GEOGRAPHY value as WKT, and it is mapped to the ClickHouse `Geometry` type, which is
/// a `Variant` of the geometric types, so the shape of the WKT text selects the variant.
/// `GEOMETRYCOLLECTION` is a valid GEOGRAPHY shape that has no ClickHouse counterpart;
/// it is rejected as unsupported instead of being silently coerced to another shape.
void insertGeographyValue(IColumn & column, const DataTypePtr & type, const BigQueryField & field, const String & text)
{
    auto & column_variant = assert_cast<ColumnVariant &>(column);
    const auto & type_variant = assert_cast<const DataTypeVariant &>(*type);

    auto insert_geometry = [&](const String & variant_name, auto & serializer)
    {
        auto discriminator = type_variant.tryGetVariantDiscriminator(variant_name);
        if (!discriminator)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot read the BigQuery GEOGRAPHY field '{}' into a column of type '{}': it has no '{}' variant",
                field.name, type->getName(), variant_name);

        auto value_column = serializer.finalize();
        column_variant.insertIntoVariantFrom(*discriminator, *value_column, 0);
    };

    auto shape = boost::to_lower_copy(text);
    boost::trim_left(shape);

    /// `read_wkt` reports a parse error as an implementation-defined exception type; translate it.
    auto read_wkt = [&](auto & out)
    {
        try
        {
            boost::geometry::read_wkt(text, out);
        }
        catch (const std::exception & e)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot parse the WKT value of the BigQuery GEOGRAPHY field '{}': {}", field.name, e.what());
        }
    };

    /// The longer prefixes are matched first, because `multilinestring` also starts with `multi`
    /// and `linestring` is a prefix of nothing else only after `multilinestring` is excluded.
    if (shape.starts_with("geometrycollection"))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The BigQuery GEOGRAPHY field '{}' contains a value that is not supported by the ClickHouse "
            "`Geometry` type: '{}'", field.name, text);

    if (shape.starts_with("multipoint"))
    {
        MultiPoint<CartesianPoint> multipoint;
        read_wkt(multipoint);
        MultiPointSerializer<CartesianPoint> serializer;
        serializer.add(multipoint);
        insert_geometry("MultiPoint", serializer);
    }
    else if (shape.starts_with("point"))
    {
        /// A ClickHouse `Point` is a `Tuple(Float64, Float64)` with no empty representation, while
        /// `POINT EMPTY` is valid WKT that `read_wkt` accepts without writing the coordinates.
        auto rest = shape.substr(strlen("point"));
        boost::trim(rest);
        if (rest == "empty")
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The BigQuery GEOGRAPHY field '{}' contains an empty point, which is not supported", field.name);

        CartesianPoint point;
        read_wkt(point);
        PointSerializer<CartesianPoint> serializer;
        serializer.add(point);
        insert_geometry("Point", serializer);
    }
    else if (shape.starts_with("multilinestring"))
    {
        MultiLineString<CartesianPoint> multilinestring;
        read_wkt(multilinestring);
        MultiLineStringSerializer<CartesianPoint> serializer;
        serializer.add(multilinestring);
        insert_geometry("MultiLineString", serializer);
    }
    else if (shape.starts_with("linestring"))
    {
        LineString<CartesianPoint> linestring;
        read_wkt(linestring);
        LineStringSerializer<CartesianPoint> serializer;
        serializer.add(linestring);
        insert_geometry("LineString", serializer);
    }
    else if (shape.starts_with("multipolygon"))
    {
        MultiPolygon<CartesianPoint> multipolygon;
        read_wkt(multipolygon);
        MultiPolygonSerializer<CartesianPoint> serializer;
        serializer.add(multipolygon);
        insert_geometry("MultiPolygon", serializer);
    }
    else if (shape.starts_with("polygon"))
    {
        Polygon<CartesianPoint> polygon;
        read_wkt(polygon);
        PolygonSerializer<CartesianPoint> serializer;
        serializer.add(polygon);
        insert_geometry("Polygon", serializer);
    }
    else
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Unexpected WKT value '{}' of the BigQuery GEOGRAPHY field '{}'", text, field.name);
}

/// Serializes one value of a `Geometry` column back to WKT for `tabledata.insertAll`.
String geographyWKT(const DataTypePtr & type, const IColumn & column, size_t row)
{
    const auto & column_variant = assert_cast<const ColumnVariant &>(column);
    const auto discriminator = column_variant.globalDiscriminatorAt(row);
    const auto & variant_type = assert_cast<const DataTypeVariant &>(*type).getVariants()[discriminator];
    /// A single row is cut out, because the converters work on whole columns.
    auto value_column = column_variant.getVariantByGlobalDiscriminator(discriminator).cut(column_variant.offsetAt(row), 1);

    String result;
    callOnGeometryDataType<CartesianPoint>(variant_type, [&](const auto & converter_type)
    {
        using Converter = typename std::decay_t<decltype(converter_type)>::Type;
        auto figures = Converter::convert(value_column);
        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        out.exceptions(std::ios::failbit);
        out << boost::geometry::wkt(figures[0]);
        result = out.str();
    });
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
        case BigQueryField::Type::JSON:
        case BigQueryField::Type::Interval:
        case BigQueryField::Type::Range:
        {
            assert_cast<ColumnString &>(column).insertData(text.data(), text.size());
            return;
        }
        case BigQueryField::Type::Geography:
        {
            insertGeographyValue(column, type, field, text);
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
            const auto element_value = element->get("v");
            /// A stored BigQuery array cannot contain NULL elements, so the element type is not Nullable.
            /// A NULL element in the response is therefore malformed input, not a value to coerce to a default.
            if (element_value.isEmpty() && !nested_type->isNullable())
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "The BigQuery array field '{}' contains a NULL element, but a BigQuery array cannot contain NULL elements",
                    field.name);
            insertNonRepeatedValue(column_array.getData(), nested_type, field, element_value);
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
        case BigQueryField::Type::JSON:
        case BigQueryField::Type::Interval:
        {
            return Poco::Dynamic::Var(String(assert_cast<const ColumnString &>(column).getDataAt(row)));
        }
        case BigQueryField::Type::Geography:
        {
            /// A `Geometry` column is a `Variant`, which represents a NULL by itself instead of being
            /// wrapped in `Nullable`, so the NULL of a NULLABLE GEOGRAPHY field is handled here.
            /// The `Variant` also carries its own NULL inside `Array(Geometry)` and for a REQUIRED field,
            /// where BigQuery accepts no NULL at all, so such a value is rejected locally instead of
            /// being sent as JSON `null` and failing remotely in `tabledata.insertAll`.
            if (assert_cast<const ColumnVariant &>(column).globalDiscriminatorAt(row) == ColumnVariant::NULL_DISCRIMINATOR)
            {
                if (field.repeated)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "An element of the BigQuery REPEATED GEOGRAPHY field '{}' is NULL, "
                        "but a BigQuery array cannot contain NULL elements", field.name);
                if (field.required)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "The BigQuery GEOGRAPHY field '{}' is REQUIRED, but the value is NULL", field.name);
                return {};
            }
            return Poco::Dynamic::Var(geographyWKT(type, column, row));
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
