#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/geometry.h>

#include <boost/geometry.hpp>
#include <boost/geometry/algorithms/perimeter.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

namespace DB
{

template <typename Point>
struct PerimeterCalculator
{
    Float64 operator()(const Point & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    Float64 operator()(const LineString<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    Float64 operator()(const Polygon<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    Float64 operator()(const MultiLineString<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    Float64 operator()(const MultiPolygon<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    Float64 operator()(const Ring<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }
};

template <typename Point>
using FunctionPerimeter = FunctionGeometry<Point, PerimeterCalculator<Point>>;

template <>
const char * FunctionPerimeter<CartesianPoint>::name = "perimeterCartesian";

template <>
const char * FunctionPerimeter<SphericalPoint>::name = "perimeterSpherical";


REGISTER_FUNCTION(Perimeter)
{
    factory.registerFunction<FunctionPerimeter<CartesianPoint>>();
    factory.registerFunction<FunctionPerimeter<SphericalPoint>>();
}

}
