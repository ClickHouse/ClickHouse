#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/geometry.h>

#include <boost/geometry.hpp>
#include <boost/geometry/algorithms/perimeter.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

namespace DB
{

template <typename Point>
struct AreaCalculator
{
    Float64 operator()(const Point & object)
    {
        return static_cast<Float64>(boost::geometry::area(object));
    }

    Float64 operator()(const LineString<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::area(object));
    }

    Float64 operator()(const Polygon<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::area(object));
    }

    Float64 operator()(const MultiLineString<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::area(object));
    }

    Float64 operator()(const MultiPolygon<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::area(object));
    }

    Float64 operator()(const Ring<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::area(object));
    }
};

template <typename Point>
using FunctionArea = FunctionGeometry<Point, AreaCalculator<Point>>;

template <>
const char * FunctionArea<CartesianPoint>::name = "areaCartesian";

template <>
const char * FunctionArea<SphericalPoint>::name = "areaSpherical";

REGISTER_FUNCTION(Area)
{
    {
        FunctionDocumentation::Description description = R"(
    Returns the area of the object.
        )";
        FunctionDocumentation::Syntax syntax = "areaCartesian(object)";
        FunctionDocumentation::Arguments arguments = {
            {"object", "geometry object", {"Geometry"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "Returns the area of the object.",
            {"Float64"}
        };
        FunctionDocumentation::Examples examples;

        FunctionDocumentation::IntroducedIn introduced_in = {25, 10};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionArea<CartesianPoint>>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
    Returns the area of the object.
        )";
        FunctionDocumentation::Syntax syntax = "areaSpherical(object)";
        FunctionDocumentation::Arguments arguments = {
            {"object", "geometry object", {"Geometry"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "Returns the area of the object.",
            {"Float64"}
        };
        FunctionDocumentation::Examples examples;
        FunctionDocumentation::IntroducedIn introduced_in = {25, 10};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionArea<SphericalPoint>>(documentation);
    }
}

}
