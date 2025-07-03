#include <variant>
#include <Common/Exception.h>
#include <Common/WKB.h>
#include <Core/Field.h>
#include <Functions/geometryConverters.h>
#include <base/types.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

String getGeometricObjectTypeName(const GeometricObject & object)
{
    if (std::holds_alternative<CartesianPoint>(object))
        return "Point";
    else if (std::holds_alternative<LineString<CartesianPoint>>(object))
        return "LineString";
    else if (std::holds_alternative<MultiLineString<CartesianPoint>>(object))
        return "MultiLineString";
    else if (std::holds_alternative<Polygon<CartesianPoint>>(object))
        return "Polygon";
    else if (std::holds_alternative<MultiPolygon<CartesianPoint>>(object))
        return "Polygon";
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown geometric object");
}

}

inline CartesianPoint readPointWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    Float64 x;
    Float64 y;
    readBinaryEndian(x, in_buffer, endian_to_read);
    readBinaryEndian(y, in_buffer, endian_to_read);
    return CartesianPoint(x, y);
}

inline LineString<CartesianPoint> readLineWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    UInt32 num_points;
    readBinaryEndian(num_points, in_buffer, endian_to_read);

    LineString<CartesianPoint> line;
    line.reserve(num_points);

    for (UInt32 i = 0; i < num_points; ++i)
    {
        line.push_back(readPointWKB(in_buffer, endian_to_read));
    }
    return line;
}

inline Ring<CartesianPoint> readRingWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    UInt32 num_points;
    readBinaryEndian(num_points, in_buffer, endian_to_read);

    Ring<CartesianPoint> ring;
    ring.reserve(num_points);

    for (UInt32 i = 0; i < num_points; ++i)
    {
        ring.push_back(readPointWKB(in_buffer, endian_to_read));
    }
    return ring;
}

inline Polygon<CartesianPoint> readPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    UInt32 num_rings;
    readBinaryEndian(num_rings, in_buffer, endian_to_read);

    Polygon<CartesianPoint> polygon;
    if (num_rings > 1)
        polygon.inners().reserve(num_rings - 1);
    for (UInt32 i = 0; i < num_rings; ++i)
    {
        if (i == 0)
            polygon.outer() = readRingWKB(in_buffer, endian_to_read);
        else
            polygon.inners().push_back(readRingWKB(in_buffer, endian_to_read));
    }
    return polygon;
}

GeometricObject parseWKBFormat(ReadBuffer & in_buffer);

MultiLineString<CartesianPoint> readMultiLineStringWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    MultiLineString<CartesianPoint> multiline;

    UInt32 num_rings;
    readBinaryEndian(num_rings, in_buffer, endian_to_read);

    multiline.reserve(num_rings);
    for (UInt32 i = 0; i < num_rings; ++i)
    {
        auto current_line = parseWKBFormat(in_buffer);
        if (!std::holds_alternative<LineString<CartesianPoint>>(current_line))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MultiLineString contains an internal type {} that differs from LineString", getGeometricObjectTypeName(current_line));
        multiline.push_back(std::get<LineString<CartesianPoint>>(current_line));
    }
    return multiline;
}

MultiPolygon<CartesianPoint> readMultiPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    MultiPolygon<CartesianPoint> multipolygon;

    UInt32 num_polygons;
    readBinaryEndian(num_polygons, in_buffer, endian_to_read);

    multipolygon.reserve(num_polygons);
    for (UInt32 i = 0; i < num_polygons; ++i)
    {
        auto current_polygon = parseWKBFormat(in_buffer);
        if (!std::holds_alternative<Polygon<CartesianPoint>>(current_polygon))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MultiPolygon contains an internal type {} that differs from Polygon", getGeometricObjectTypeName(current_polygon));
        multipolygon.push_back(std::get<Polygon<CartesianPoint>>(current_polygon));
    }
    return multipolygon;
}

GeometricObject parseWKBFormat(ReadBuffer & in_buffer)
{
    char little_endian;
    if (!in_buffer.read(little_endian) || (little_endian != 0 && little_endian != 1))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading WKB format: Incorrect first flag");

    std::endian endian_to_read = little_endian ? std::endian::little : std::endian::big;

    UInt32 geom_type;
    readBinaryEndian(geom_type, in_buffer, endian_to_read);

    switch (static_cast<WKBGeometry>(geom_type))
    {
        case WKBGeometry::Point:
            return readPointWKB(in_buffer, endian_to_read);
        case WKBGeometry::LineString:
            return readLineWKB(in_buffer, endian_to_read);
        case WKBGeometry::Polygon:
            return readPolygonWKB(in_buffer, endian_to_read);
        case WKBGeometry::MultiLineString:
            return readMultiLineStringWKB(in_buffer, endian_to_read);
        case WKBGeometry::MultiPolygon:
            return readMultiPolygonWKB(in_buffer, endian_to_read);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect geometry type {}", geom_type);
}

}
