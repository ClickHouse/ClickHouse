#include <Processors/Formats/Impl/ArrowGeoTypes.h>

#include <bit>
#include <cstring>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <base/types.h>
#include <Common/Exception.h>

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
        Poco::JSON::Object::Ptr obj = geo_json.value();
        if (!obj->has("columns"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect geo json metadata");

        Poco::JSON::Object::Ptr columns = obj->getObject("columns");
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
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown geo type {}", type);

            geo_columns[column_name] = GeoColumnMetadata{.encoding = geo_encoding, .type = result_type};
        }
    }

    return geo_columns;
}

Point readPointWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    double x;
    double y;
    readBinaryEndian(x, in_buffer, endian_to_read);
    readBinaryEndian(y, in_buffer, endian_to_read);
    return Point{.x = x, .y = y};
}

Line readLineWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    int num_points;
    readBinaryEndian(num_points, in_buffer, endian_to_read);

    Line line;
    for (int i = 0; i < num_points; ++i)
    {
        line.push_back(readPointWKB(in_buffer, endian_to_read));
    }
    return line;
}

Polygon readPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    int num_points;
    readBinaryEndian(num_points, in_buffer, endian_to_read);

    Polygon polygon;
    for (int i = 0; i < num_points; ++i)
    {
        polygon.push_back(readLineWKB(in_buffer, endian_to_read));
    }
    return polygon;
}

GeometricObject parseWKBFormat(ReadBuffer & in_buffer)
{
    char little_endian;
    if (!in_buffer.read(little_endian))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading WKB format: Incorrect first flag");

    std::endian endian_to_read = little_endian ? std::endian::little : std::endian::big;
    int geom_type;

    readBinaryEndian(geom_type, in_buffer, endian_to_read);

    if (geom_type == 1)
        return readPointWKB(in_buffer, endian_to_read);
    else if (geom_type == 2)
        return readLineWKB(in_buffer, endian_to_read);
    else if (geom_type == 3)
        return readPolygonWKB(in_buffer, endian_to_read);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading WKB format: Incorrect geometry type");
}

Point parseWKTPoint(ReadBuffer & in_buffer)
{
    double x;
    double y;
    readFloatText(x, in_buffer);
    in_buffer.ignore();
    readFloatText(y, in_buffer);
    in_buffer.ignore();
    return {x, y};
}

Line parseWKTLine(ReadBuffer & in_buffer)
{
    char ch;
    Line ls;
    while (true)
    {
        double x;
        double y;
        readFloatText(x, in_buffer);
        in_buffer.ignore();
        readFloatText(y, in_buffer);
        ls.push_back({x, y});
        readBinary(ch, in_buffer);
        if (ch == ')')
            break;
        in_buffer.ignore();
    }
    return ls;
}

Polygon parseWKTPolygon(ReadBuffer & in_buffer)
{
    char ch;
    Polygon poly;
    while (true)
    {
        in_buffer.ignore();
        std::vector<Point> ring;
        while (true)
        {
            double x;
            double y;
            readFloatText(x, in_buffer);
            in_buffer.ignore();
            readFloatText(y, in_buffer);
            ring.push_back({x, y});
            readBinary(ch, in_buffer);
            if (ch == ')')
                break;
            in_buffer.ignore();
        }
        poly.push_back(ring);
        readBinary(ch, in_buffer);
        if (ch == ')')
            break;
        in_buffer.ignore();
    }
    return poly;
}

GeometricObject parseWKTFormat(ReadBuffer & in_buffer)
{
    std::string type;
    while (true)
    {
        char current_symbol;
        readBinary(current_symbol, in_buffer);
        if (current_symbol == '(')
            break;
        type.push_back(current_symbol);
    }

    if (type == "POINT")
        return parseWKTPoint(in_buffer);
    if (type == "LINESTRING")
        return parseWKTLine(in_buffer);
    if (type == "POLYGON")
        return parseWKTPolygon(in_buffer);
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading WKT format: type {}", type);
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
    if (!std::holds_alternative<Point>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected point");

    const auto & point = std::get<Point>(object);
    point_column_data_x.push_back(point.x);
    point_column_data_y.push_back(point.y);
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
    if (!std::holds_alternative<Line>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected point");

    const auto & line = std::get<Line>(object);
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
    if (!std::holds_alternative<Polygon>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected point");

    const auto & polygon = std::get<Polygon>(object);
    for (const auto & line : polygon)
    {
        line_column_builder.appendObject(line);
    }
    offset += polygon.size();
    offsets.push_back(offset);
}

ColumnWithTypeAndName PolygonColumnBuilder::getResultColumn()
{
    auto all_points_column = line_column_builder.getResultColumn();
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
