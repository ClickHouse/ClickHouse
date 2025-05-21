#include "Common/Exception.h"
#include <Common/WKB.h>
#include "Functions/geometryConverters.h"

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
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
    Int32 num_points;
    readBinaryEndian(num_points, in_buffer, endian_to_read);

    LineString<CartesianPoint> line;
    for (int i = 0; i < num_points; ++i)
    {
        line.push_back(readPointWKB(in_buffer, endian_to_read));
    }
    return line;
}

inline Polygon<CartesianPoint> readPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    Int32 num_lines;
    readBinaryEndian(num_lines, in_buffer, endian_to_read);

    Polygon<CartesianPoint> polygon;
    for (int i = 0; i < num_lines; ++i)
    {
        auto parsed_points = readLineWKB(in_buffer, endian_to_read);
        if (i == 0)
        {
            polygon.outer().reserve(parsed_points.size());
            for (auto && point : parsed_points)
                polygon.outer().push_back(point);
        }
        else
        {
            polygon.inners().push_back({});
            polygon.inners().back().reserve(parsed_points.size());
            for (auto && point : parsed_points)
                polygon.inners().back().push_back(point);
        }
    }
    return polygon;
}

GeometricObject parseWKBFormat(ReadBuffer & in_buffer);

MultiLineString<CartesianPoint> readMultiLineStringWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    MultiLineString<CartesianPoint> multiline;

    Int32 num_lines;
    readBinaryEndian(num_lines, in_buffer, endian_to_read);

    for (int i = 0; i < num_lines; ++i)
        multiline.push_back(std::get<LineString<CartesianPoint>>(parseWKBFormat(in_buffer)));

    return multiline;
}

MultiPolygon<CartesianPoint> readMultiPolygonWKB(ReadBuffer & in_buffer, std::endian endian_to_read)
{
    MultiPolygon<CartesianPoint> multipolygon;

    Int32 num_polygons;
    readBinaryEndian(num_polygons, in_buffer, endian_to_read);

    for (int i = 0; i < num_polygons; ++i)
        multipolygon.push_back(std::get<Polygon<CartesianPoint>>(parseWKBFormat(in_buffer)));

    return multipolygon;
}

GeometricObject parseWKBFormat(ReadBuffer & in_buffer)
{
    char little_endian;
    if (!in_buffer.read(little_endian) || (little_endian != 0 && little_endian != 1))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading WKB format: Incorrect first flag");

    std::endian endian_to_read = little_endian ? std::endian::little : std::endian::big;
    Int32 geom_type;

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
