#pragma once

#include <variant>
#include <vector>

#include <IO/ReadBuffer.h>
#include <Functions/geometryConverters.h>

namespace DB
{

enum class WKBGeometry : UInt32
{
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiLineString = 5,
    MultiPolygon = 6
};

using GeometricObject = std::variant<
    CartesianPoint,
    LineString<CartesianPoint>,
    MultiLineString<CartesianPoint>,
    Polygon<CartesianPoint>,
    MultiPolygon<CartesianPoint>>;

GeometricObject parseWKBFormat(ReadBuffer & in_buffer);

}
