#include <Processors/Formats/Impl/ArrowGeoTypes.h>

#include <bit>
#include <cstring>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Functions/geometryConverters.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>

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

inline CartesianPoint parseWKTPoint(ReadBuffer & in_buffer)
{
    Float64 x;
    Float64 y;
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

inline LineString<CartesianPoint> parseWKTLine(ReadBuffer & in_buffer)
{
    LineString<CartesianPoint> ls;
    readOpenBracket(in_buffer);
    while (true)
    {
        ls.push_back(parseWKTPoint(in_buffer));
        if (readItemEnding(in_buffer))
            break;
    }
    return ls;
}

inline Ring<CartesianPoint> parseWKTRing(ReadBuffer & in_buffer)
{
    Ring<CartesianPoint> ring;
    readOpenBracket(in_buffer);
    while (true)
    {
        ring.push_back(parseWKTPoint(in_buffer));
        if (readItemEnding(in_buffer))
            break;
    }
    return ring;
}

inline Polygon<CartesianPoint> parseWKTPolygon(ReadBuffer & in_buffer)
{
    Polygon<CartesianPoint> poly;
    readOpenBracket(in_buffer);
    bool should_complete_outer = true;
    while (true)
    {
        auto parsed_line = parseWKTRing(in_buffer);
        if (should_complete_outer)
        {
            should_complete_outer = false;
            poly.outer() = std::move(parsed_line);
        }
        else
        {
            poly.inners().push_back(std::move(parsed_line));
        }
        if (readItemEnding(in_buffer))
            break;
    }
    return poly;
}

inline MultiPolygon<CartesianPoint> parseWKTMultiPolygon(ReadBuffer & in_buffer)
{
    MultiPolygon<CartesianPoint> poly;
    readOpenBracket(in_buffer);
    while (true)
    {
        poly.push_back(parseWKTPolygon(in_buffer));
        if (readItemEnding(in_buffer))
            break;
    }
    return poly;
}

GeometricObject parseWKTFormat(ReadBuffer & in_buffer)
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

void PointColumnBuilder::appendObject(const GeometricObject & object)
{
    if (!std::holds_alternative<CartesianPoint>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected point");

    const auto & point = std::get<CartesianPoint>(object);
    point_column_data_x.push_back(point.x());
    point_column_data_y.push_back(point.y());
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

    auto tuple_type = DataTypeFactory::instance().get("Point");
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
    if (!std::holds_alternative<LineString<CartesianPoint>>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected line string");

    const auto & line = std::get<LineString<CartesianPoint>>(object);
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

    auto array_type = DataTypeFactory::instance().get("LineString");
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
    if (!std::holds_alternative<Polygon<CartesianPoint>>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected polygon");

    const auto & polygon = std::get<Polygon<CartesianPoint>>(object);
    line_column_builder.appendObject(LineString<CartesianPoint>(polygon.outer().begin(), polygon.outer().end()));

    for (const auto & inner_circle : polygon.inners())
        line_column_builder.appendObject(LineString<CartesianPoint>(inner_circle.begin(), inner_circle.end()));
    offset += polygon.inners().size() + 1;
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

    auto array_type = DataTypeFactory::instance().get("Polygon");
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
    if (!std::holds_alternative<MultiLineString<CartesianPoint>>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multiline");

    const auto & multilinestring = std::get<MultiLineString<CartesianPoint>>(object);
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

    auto array_type = DataTypeFactory::instance().get("MultiLineString");
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
    if (!std::holds_alternative<MultiPolygon<CartesianPoint>>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multi polygon");

    const auto & multipolygon = std::get<MultiPolygon<CartesianPoint>>(object);
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

    auto array_type = DataTypeFactory::instance().get("MultiPolygon");
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

void GeoColumnBuilder::appendDefault()
{
    geomery_column_builder->appendDefault();
}


ColumnWithTypeAndName GeoColumnBuilder::getResultColumn()
{
    return geomery_column_builder->getResultColumn();
}

}
