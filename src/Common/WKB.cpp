#include <Common/WKB.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
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

}
