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
extern const int LOGICAL_ERROR;
}

#if USE_ARROW
const std::string * extractGeoMetadata(std::shared_ptr<const arrow::KeyValueMetadata> metadata)
{
    if (!metadata)
        return nullptr;

    for (Int64 i = 0; i < metadata->size(); ++i)
        if (metadata->key(i) == "geo")
            return &metadata->value(i);

    return nullptr;
}
#endif

std::unordered_map<String, GeoColumnMetadata> parseGeoMetadataEncoding(const std::string * geo_json_str)
{
    if (!geo_json_str)
        return {};

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(*geo_json_str);
    const Poco::JSON::Object::Ptr & obj = result.extract<Poco::JSON::Object::Ptr>();

    if (!obj->has("columns"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect geo json metadata: missing \"columns\"");
    const Poco::JSON::Object::Ptr & columns = obj->getObject("columns");

    std::unordered_map<String, GeoColumnMetadata> geo_columns;

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse does not support multiple geo types in one column");

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

DataTypePtr getGeoDataType(GeoType type)
{
    switch (type)
    {
        case GeoType::Point: return DataTypeFactory::instance().get("Point");
        case GeoType::LineString: return DataTypeFactory::instance().get("LineString");
        case GeoType::Polygon: return DataTypeFactory::instance().get("Polygon");
        case GeoType::MultiLineString: return DataTypeFactory::instance().get("MultiLineString");
        case GeoType::MultiPolygon: return DataTypeFactory::instance().get("MultiPolygon");
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid GeoType: {}", uint8_t(type));
}

static void appendPointToGeoColumn(const CartesianPoint & point, IColumn & col)
{
    auto & tuple = assert_cast<ColumnTuple &>(col);
    assert_cast<ColumnFloat64 &>(tuple.getColumn(0)).getData().push_back(point.x());
    assert_cast<ColumnFloat64 &>(tuple.getColumn(1)).getData().push_back(point.y());
}

static void appendLineStringToGeoColumn(const LineString<CartesianPoint> & line, IColumn & col)
{
    auto & array = assert_cast<ColumnArray &>(col);

    for (const auto & point : line)
        appendPointToGeoColumn(point, array.getData());

    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + line.size());
}

static void appendPolygonToGeoColumn(const Polygon<CartesianPoint> & polygon, IColumn & col)
{
    auto & array = assert_cast<ColumnArray &>(col);

    appendLineStringToGeoColumn(LineString<CartesianPoint>(polygon.outer().begin(), polygon.outer().end()), array.getData());

    for (const auto & inner_circle : polygon.inners())
        appendLineStringToGeoColumn(LineString<CartesianPoint>(inner_circle.begin(), inner_circle.end()), array.getData());

    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + polygon.inners().size() + 1);
}

static void appendMultiLineStringToGeoColumn(const MultiLineString<CartesianPoint> & multilinestring, IColumn & col)
{
    auto & array = assert_cast<ColumnArray &>(col);

    for (const auto & line : multilinestring)
        appendLineStringToGeoColumn(line, array.getData());

    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + multilinestring.size());
}

static void appendMultiPolygonToGeoColumn(const MultiPolygon<CartesianPoint> & multipolygon, IColumn & col)
{
    auto & array = assert_cast<ColumnArray &>(col);

    for (const auto & polygon : multipolygon)
        appendPolygonToGeoColumn(polygon, array.getData());

    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + multipolygon.size());
}

void appendObjectToGeoColumn(const GeometricObject & object, GeoType type, IColumn & col)
{
    switch (type)
    {
        case GeoType::Point:
            if (!std::holds_alternative<CartesianPoint>(object))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected point");
            appendPointToGeoColumn(std::get<CartesianPoint>(object), col);
            return;
        case GeoType::LineString:
            if (!std::holds_alternative<LineString<CartesianPoint>>(object))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected line string");
            appendLineStringToGeoColumn(std::get<LineString<CartesianPoint>>(object), col);
            return;
        case GeoType::Polygon:
            if (!std::holds_alternative<Polygon<CartesianPoint>>(object))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multiline");
            appendPolygonToGeoColumn(std::get<Polygon<CartesianPoint>>(object), col);
            return;
        case GeoType::MultiLineString:
            if (!std::holds_alternative<MultiLineString<CartesianPoint>>(object))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multiline");
            appendMultiLineStringToGeoColumn(std::get<MultiLineString<CartesianPoint>>(object), col);
            return;
        case GeoType::MultiPolygon:
            if (!std::holds_alternative<MultiPolygon<CartesianPoint>>(object))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Types in parquet mismatched - expected multi polygon");
            appendMultiPolygonToGeoColumn(std::get<MultiPolygon<CartesianPoint>>(object), col);
            return;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid GeoType: {}", uint8_t(type));
}

}
