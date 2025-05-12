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

inline ArrowPoint readPointWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    double x;
    double y;
    readBinaryEndian(x, in_buffer, endian_to_read);
    readBinaryEndian(y, in_buffer, endian_to_read);
    return ArrowPoint{.x = x, .y = y};
}

inline ArrowLineString readLineWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    int num_points;
    readBinaryEndian(num_points, in_buffer, endian_to_read);

    ArrowLineString line;
    for (int i = 0; i < num_points; ++i)
    {
        line.push_back(readPointWKB(in_buffer, endian_to_read));
    }
    return line;
}

inline ArrowPolygon readPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    int num_lines;
    readBinaryEndian(num_lines, in_buffer, endian_to_read);

    ArrowPolygon polygon;
    for (int i = 0; i < num_lines; ++i)
    {
        auto parsed_points = readLineWKB(in_buffer, endian_to_read);
        polygon.push_back(std::move(parsed_points));
    }
    return polygon;
}

ArrowGeometricObject parseWKBFormat(ReadBuffer & in_buffer);

ArrowMultiLineString readMultiLineStringWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    ArrowMultiLineString multiline;

    int num_lines;
    readBinaryEndian(num_lines, in_buffer, endian_to_read);

    for (int i = 0; i < num_lines; ++i)
        multiline.push_back(std::get<ArrowLineString>(parseWKBFormat(in_buffer)));

    return multiline;
}

ArrowMultiPolygon readMultiPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    ArrowMultiPolygon multipolygon;

    int num_polygons;
    readBinaryEndian(num_polygons, in_buffer, endian_to_read);

    for (int i = 0; i < num_polygons; ++i)
        multipolygon.push_back(std::get<ArrowPolygon>(parseWKBFormat(in_buffer)));

    return multipolygon;
}

ArrowGeometricObject parseWKBFormat(ReadBuffer & in_buffer)
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

inline ArrowPoint parseWKTPoint(ReadBuffer & in_buffer)
{
    double x;
    double y;
    char ch;
    while (true)
    {
        if (!in_buffer.peek(ch))
            break;
        if (ch != ' ')
            break;
        in_buffer.ignore();
    }
    tryReadFloatText(x, in_buffer);
    in_buffer.ignore();
    readFloatText(y, in_buffer);
    return {x, y};
}

inline void readOpenBracket(ReadBuffer & in_buffer)
{
    while (true)
    {
        char ch;
        readBinary(ch, in_buffer);
        if (ch == '(')
            break;
    }
}

inline bool readItemEnding(ReadBuffer & in_buffer)
{
    char ch;
    while (true)
    {
        readBinary(ch, in_buffer);
        if (ch == ')')
            return true;

        if (ch == ',')
            return false;
    }
}

inline ArrowLineString parseWKTLine(ReadBuffer & in_buffer)
{
    ArrowLineString ls;
    readOpenBracket(in_buffer);
    while (true)
    {
        ls.push_back(parseWKTPoint(in_buffer));
        if (readItemEnding(in_buffer))
            break;
    }
    return ls;
}

inline ArrowPolygon parseWKTPolygon(ReadBuffer & in_buffer)
{
    ArrowPolygon poly;
    readOpenBracket(in_buffer);
    while (true)
    {
        poly.push_back(parseWKTLine(in_buffer));
        if (readItemEnding(in_buffer))
            break;
    }
    return poly;
}

inline ArrowMultiPolygon parseWKTMultiPolygon(ReadBuffer & in_buffer)
{
    ArrowMultiPolygon poly;
    readOpenBracket(in_buffer);
    while (true)
    {
        poly.push_back(parseWKTPolygon(in_buffer));
        if (readItemEnding(in_buffer))
            break;
    }
    return poly;
}

ArrowGeometricObject parseWKTFormat(ReadBuffer & in_buffer)
{
    std::string type;
    while (true)
    {
        char current_symbol;
        if (!in_buffer.peek(current_symbol))
            break;
        if (current_symbol == '(')
            break;
        type.push_back(current_symbol);
        in_buffer.ignore();
    }

    while (type.back() == ' ')
        type.pop_back();

    if (type == "POINT")
    {
        readOpenBracket(in_buffer);
        return parseWKTPoint(in_buffer);
    }
    if (type == "LINESTRING")
        return parseWKTLine(in_buffer);
    if (type == "POLYGON")
        return parseWKTPolygon(in_buffer);
    if (type == "MULTILINESTRING")
        return parseWKTPolygon(in_buffer);
    if (type == "MULTIPOLYGON")
        return parseWKTMultiPolygon(in_buffer);

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

void PointColumnBuilder::appendObject(const ArrowGeometricObject & object)
{
    if (!std::holds_alternative<ArrowPoint>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected point");

    const auto & point = std::get<ArrowPoint>(object);
    point_column_data_x.push_back(point.x);
    point_column_data_y.push_back(point.y);
}

void PointColumnBuilder::appendDefault()
{
    point_column_data_x.push_back(0);
    point_column_data_y.push_back(0);
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

void LineColumnBuilder::appendObject(const ArrowGeometricObject & object)
{
    if (!std::holds_alternative<ArrowLineString>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected line string");

    const auto & line = std::get<ArrowLineString>(object);
    for (const auto & point : line)
    {
        point_column_builder.appendObject(point);
    }
    offset += line.size();
    offsets.push_back(offset);
}

void LineColumnBuilder::appendDefault()
{
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

void PolygonColumnBuilder::appendObject(const ArrowGeometricObject & object)
{
    if (!std::holds_alternative<ArrowPolygon>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected polygon");

    const auto & polygon = std::get<ArrowPolygon>(object);
    for (const auto & inner_circle : polygon)
        line_column_builder.appendObject(inner_circle);
    offset += polygon.size();
    offsets.push_back(offset);
}

void PolygonColumnBuilder::appendDefault()
{
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

void MultiLineStringColumnBuilder::appendObject(const ArrowGeometricObject & object)
{
    if (!std::holds_alternative<ArrowMultiLineString>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multiline");

    const auto & multilinestring = std::get<ArrowMultiLineString>(object);
    for (const auto & line : multilinestring)
        line_column_builder.appendObject(line);

    offset += multilinestring.size();
    offsets.push_back(offset);
}

void MultiLineStringColumnBuilder::appendDefault()
{
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

void MultiPolygonColumnBuilder::appendObject(const ArrowGeometricObject & object)
{
    if (!std::holds_alternative<ArrowMultiPolygon>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multi polygon");

    const auto & multipolygon = std::get<ArrowMultiPolygon>(object);
    for (const auto & polygon : multipolygon)
        polygon_column_builder.appendObject(polygon);

    offset += multipolygon.size();
    offsets.push_back(offset);
}

void MultiPolygonColumnBuilder::appendDefault()
{
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

void GeoColumnBuilder::appendObject(const ArrowGeometricObject & object)
{
    geomery_column_builder->appendObject(object);
}

void GeoColumnBuilder::appendDefault()
{
    geomery_column_builder->appendDefault();
}


ColumnWithTypeAndName GeoColumnBuilder::getResultColumn()
{
    return geomery_column_builder->getResultColumn();
}

}
