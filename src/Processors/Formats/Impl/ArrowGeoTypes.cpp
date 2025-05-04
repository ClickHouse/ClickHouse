#include <Processors/Formats/Impl/ArrowGeoTypes.h>

#include <bit>
#include <cstring>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <base/types.h>
#include <Common/Exception.h>
#include "Functions/geometryConverters.h"
#include "IO/ReadBufferFromString.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

#if USE_ARROW
std::optional<Poco::JSON::Object::Ptr> extractGeoMetadata(std::shared_ptr<const arrow::KeyValueMetadata> metadata)
{
    if (!metadata)
        return std::nullopt;

    for (Int64 i = 0; i < metadata->size(); ++i)
    {
        if (metadata->key(i) == "geo")
        {
            const auto & value = metadata->value(i);
            Poco::JSON::Parser parser;
            Poco::Dynamic::Var result = parser.parse(value);
            Poco::JSON::Object::Ptr obj = result.extract<Poco::JSON::Object::Ptr>();
            return obj;
        }
    }
    return std::nullopt;
}
#endif

std::unordered_map<String, GeoColumnMetadata> parseGeoMetadataEncoding(std::optional<Poco::JSON::Object::Ptr> geo_json)
{
    std::unordered_map<String, GeoColumnMetadata> geo_columns;

    if (geo_json.has_value())
    {
        const Poco::JSON::Object::Ptr & obj = geo_json.value();
        if (!obj->has("columns"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect geo json metadata");

        const Poco::JSON::Object::Ptr & columns = obj->getObject("columns");
        for (const auto & column_entry : *columns)
        {
            const std::string & column_name = column_entry.first;
            Poco::JSON::Object::Ptr column_obj = column_entry.second.extract<Poco::JSON::Object::Ptr>();

            String encoding_name = column_obj->getValue<std::string>("encoding");
            GeoEncoding geo_encoding;

            if (encoding_name == "WKB")
                geo_encoding = GeoEncoding::WKB;
            else if (encoding_name == "WKT")
                geo_encoding = GeoEncoding::WKT;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect encoding name in geo json metadata: {}", encoding_name);

            Poco::JSON::Array::Ptr types = column_obj->getArray("geometry_types");
            if (types->size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse does not support different types in one column");

            String type = types->getElement<std::string>(0);
            GeoType result_type;

            if (type == "Point")
                result_type = GeoType::Point;
            else if (type == "LineString")
                result_type = GeoType::LineString;
            else if (type == "Polygon")
                result_type = GeoType::Polygon;
            else if (type == "MultiLineString")
                result_type = GeoType::MultiLineString;
            else if (type == "MultiPolygon")
                result_type = GeoType::MultiPolygon;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown geo type {}", type);

            geo_columns[column_name] = GeoColumnMetadata{.encoding = geo_encoding, .type = result_type};
        }
    }

    return geo_columns;
}

CartesianPoint readPointWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    double x;
    double y;
    readBinaryEndian(x, in_buffer, endian_to_read);
    readBinaryEndian(y, in_buffer, endian_to_read);
    return CartesianPoint(x, y);
}

CartesianLineString readLineWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    int num_points;
    readBinaryEndian(num_points, in_buffer, endian_to_read);

    CartesianLineString line;
    for (int i = 0; i < num_points; ++i)
    {
        line.push_back(readPointWKB(in_buffer, endian_to_read));
    }
    return line;
}

CartesianPolygon readPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    int num_lines;
    readBinaryEndian(num_lines, in_buffer, endian_to_read);

    CartesianPolygon polygon;
    {
        auto parsed_points = readLineWKB(in_buffer, endian_to_read);
        for (const auto & point : parsed_points)
            polygon.outer().push_back(point);
    }

    for (int i = 1; i < num_lines; ++i)
    {
        auto parsed_points = readLineWKB(in_buffer, endian_to_read);
        polygon.inners().push_back({});
        for (const auto & point : parsed_points)
            polygon.inners().back().push_back(point);
    }
    return polygon;
}

GeometricObject parseWKBFormat(ReadBuffer & in_buffer);

CartesianMultiLineString readMultiLineStringWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    CartesianMultiLineString multiline;

    int num_lines;
    readBinaryEndian(num_lines, in_buffer, endian_to_read);

    for (int i = 0; i < num_lines; ++i)
        multiline.push_back(std::get<CartesianLineString>(parseWKBFormat(in_buffer)));

    return multiline;
}

CartesianMultiPolygon readMultiPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    CartesianMultiPolygon multipolygon;

    int num_polygons;
    readBinaryEndian(num_polygons, in_buffer, endian_to_read);

    for (int i = 0; i < num_polygons; ++i)
        multipolygon.push_back(std::get<CartesianPolygon>(parseWKBFormat(in_buffer)));

    return multipolygon;
}

GeometricObject parseWKBFormat(ReadBuffer & in_buffer)
{
    char little_endian;
    if (!in_buffer.read(little_endian))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading WKB format: Incorrect first flag");

    std::endian endian_to_read = little_endian ? std::endian::little : std::endian::big;
    int geom_type;

    readBinaryEndian(geom_type, in_buffer, endian_to_read);

    switch (geom_type)
    {
        case 1:
            return readPointWKB(in_buffer, endian_to_read);
        case 2:
            return readLineWKB(in_buffer, endian_to_read);
        case 3:
            return readPolygonWKB(in_buffer, endian_to_read);
        case 5:
            return readMultiLineStringWKB(in_buffer, endian_to_read);
        case 6:
            return readMultiPolygonWKB(in_buffer, endian_to_read);
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading WKB format: Incorrect geometry type");
    }
}

GeometricObject parseWKBFormat(const String & input)
{
    auto in_buffer = ReadBufferFromString(input);

    return parseWKBFormat(in_buffer);
}

GeometricObject parseWKTFormat(const String & input)
{
    if (input.starts_with("POINT"))
    {
        CartesianPoint point;
        boost::geometry::read_wkt(input, point);
        return point;
    }
    if (input.starts_with("POLYGON"))
    {
        CartesianPolygon polygon;
        boost::geometry::read_wkt(input, polygon);
        return polygon;
    }
    if (input.starts_with("LINESTRING"))
    {
        CartesianLineString linestring;
        boost::geometry::read_wkt(input, linestring);
        return linestring;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown geometry object in WKT {}", input);
}

PointColumnBuilder::PointColumnBuilder(const String & name_)
    : point_column_x(ColumnFloat64::create())
    , point_column_y(ColumnFloat64::create())
    , point_column_data_x(point_column_x->getData())
    , point_column_data_y(point_column_y->getData())
    , name(name_)
{
}

void PointColumnBuilder::appendObject(const GeometricObject & object)
{
    if (!std::holds_alternative<CartesianPoint>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected point");

    const auto & point = std::get<CartesianPoint>(object);
    point_column_data_x.push_back(point.x());
    point_column_data_y.push_back(point.y());
}

ColumnWithTypeAndName PointColumnBuilder::getResultColumn()
{
    ColumnPtr result_x = point_column_x->getPtr();
    ColumnPtr result_y = point_column_y->getPtr();
    auto column = ColumnTuple::create(Columns{result_x, result_y});

    DataTypePtr type_x = std::make_shared<DataTypeFloat64>();
    DataTypePtr type_y = std::make_shared<DataTypeFloat64>();

    auto tuple_type = std::make_shared<DataTypeTuple>(std::vector{type_x, type_y});
    return {std::move(column), tuple_type, name};
}

LineColumnBuilder::LineColumnBuilder(const String & name_)
    : offsets_column(ColumnVector<ColumnArray::Offset>::create())
    , offsets(offsets_column->getData())
    , point_column_builder("")
    , name(name_)
{
}

void LineColumnBuilder::appendObject(const GeometricObject & object)
{
    if (!std::holds_alternative<CartesianLineString>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected line string");

    const auto & line = std::get<CartesianLineString>(object);
    for (const auto & point : line)
    {
        point_column_builder.appendObject(point);
    }
    offset += line.size();
    offsets.push_back(offset);
}

ColumnWithTypeAndName LineColumnBuilder::getResultColumn()
{
    auto all_points_column = point_column_builder.getResultColumn();
    std::cerr << "compare linestring sizes " << all_points_column.column->size() << ' ' << offsets.size() << '\n';
    auto array_column = ColumnArray::create(all_points_column.column, offsets_column->getPtr());

    auto array_type = std::make_shared<DataTypeArray>(all_points_column.type);
    return {std::move(array_column), array_type, name};
}

PolygonColumnBuilder::PolygonColumnBuilder(const String & name_)
    : offsets_column(ColumnVector<ColumnArray::Offset>::create())
    , offsets(offsets_column->getData())
    , line_column_builder("")
    , name(name_)
{
}

void PolygonColumnBuilder::appendObject(const GeometricObject & object)
{
    if (!std::holds_alternative<CartesianPolygon>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected polygon");

    const auto & polygon = std::get<CartesianPolygon>(object);
    line_column_builder.appendObject(CartesianLineString(polygon.outer().begin(), polygon.outer().end()));
    for (const auto & inner_circle : polygon.inners())
        line_column_builder.appendObject(CartesianLineString(inner_circle.begin(), inner_circle.end()));
    offset += 1 + polygon.inners().size();
    offsets.push_back(offset);
}

ColumnWithTypeAndName PolygonColumnBuilder::getResultColumn()
{
    auto all_points_column = line_column_builder.getResultColumn();
    auto array_column = ColumnArray::create(all_points_column.column, offsets_column->getPtr());

    auto array_type = std::make_shared<DataTypeArray>(all_points_column.type);
    return {std::move(array_column), array_type, name};
}

MultiLineStringColumnBuilder::MultiLineStringColumnBuilder(const String & name_)
    : offsets_column(ColumnVector<ColumnArray::Offset>::create())
    , offsets(offsets_column->getData())
    , line_column_builder("")
    , name(name_)
{
}

void MultiLineStringColumnBuilder::appendObject(const GeometricObject & object)
{
    if (!std::holds_alternative<CartesianMultiLineString>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multiline");

    const auto & multilinestring = std::get<CartesianMultiLineString>(object);
    for (const auto & line : multilinestring)
        line_column_builder.appendObject(line);

    offset += multilinestring.size();
    offsets.push_back(offset);
}

ColumnWithTypeAndName MultiLineStringColumnBuilder::getResultColumn()
{
    auto all_points_column = line_column_builder.getResultColumn();
    auto array_column = ColumnArray::create(all_points_column.column, offsets_column->getPtr());

    auto array_type = std::make_shared<DataTypeArray>(all_points_column.type);
    return {std::move(array_column), array_type, name};
}

MultiPolygonColumnBuilder::MultiPolygonColumnBuilder(const String & name_)
    : offsets_column(ColumnVector<ColumnArray::Offset>::create())
    , offsets(offsets_column->getData())
    , polygon_column_builder("")
    , name(name_)
{
}

void MultiPolygonColumnBuilder::appendObject(const GeometricObject & object)
{
    if (!std::holds_alternative<CartesianMultiPolygon>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multi polygon");

    const auto & multipolygon = std::get<CartesianMultiPolygon>(object);
    for (const auto & polygon : multipolygon)
        polygon_column_builder.appendObject(polygon);

    offset += multipolygon.size();
    offsets.push_back(offset);
}

ColumnWithTypeAndName MultiPolygonColumnBuilder::getResultColumn()
{
    auto all_points_column = polygon_column_builder.getResultColumn();
    auto array_column = ColumnArray::create(all_points_column.column, offsets_column->getPtr());

    auto array_type = std::make_shared<DataTypeArray>(all_points_column.type);
    return {std::move(array_column), array_type, name};
}

GeoColumnBuilder::GeoColumnBuilder(const String & name_, GeoType type_)
    : name(name_)
{
    switch (type_)
    {
        case GeoType::Point:
            geomery_column_builder = std::make_unique<PointColumnBuilder>(name);
            break;
        case GeoType::LineString:
            geomery_column_builder = std::make_unique<LineColumnBuilder>(name);
            break;
        case GeoType::Polygon:
            geomery_column_builder = std::make_unique<PolygonColumnBuilder>(name);
            break;
        case GeoType::MultiLineString:
            geomery_column_builder = std::make_unique<MultiLineStringColumnBuilder>(name);
            break;
        case GeoType::MultiPolygon:
            geomery_column_builder = std::make_unique<MultiPolygonColumnBuilder>(name);
            break;
    }
}

void GeoColumnBuilder::appendObject(const GeometricObject & object)
{
    geomery_column_builder->appendObject(object);
}

ColumnWithTypeAndName GeoColumnBuilder::getResultColumn()
{
    return geomery_column_builder->getResultColumn();
}

}
