#pragma once

#include <variant>
#include <vector>

#include <IO/ReadBuffer.h>

namespace DB
{

struct ArrowPoint
{
    double x;
    double y;
};

using ArrowLineString = std::vector<ArrowPoint>;
using ArrowPolygon = std::vector<std::vector<ArrowPoint>>;
using ArrowMultiLineString = std::vector<ArrowLineString>;
using ArrowMultiPolygon = std::vector<ArrowPolygon>;

using ArrowGeometricObject = std::variant<ArrowPoint, ArrowLineString, ArrowPolygon, ArrowMultiPolygon>;

ArrowGeometricObject parseWKBFormat(ReadBuffer & in_buffer);

}
