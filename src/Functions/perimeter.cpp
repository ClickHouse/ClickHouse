#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/geometry.h>

#include <boost/geometry/algorithms/length.hpp>
#include <boost/geometry/algorithms/perimeter.hpp>

namespace DB
{

template <typename Point>
struct PerimeterCalculator
{
    Float64 operator()(const Point & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    Float64 operator()(const MultiPoint<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    /// `boost::geometry::perimeter` of a linear geometry is 0, so the perimeter of a line is its length.
    Float64 operator()(const LineString<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::length(object));
    }

    Float64 operator()(const Polygon<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::perimeter(object));
    }

    Float64 operator()(const MultiLineString<Point> & object)
    {
        return static_cast<Float64>(boost::geometry::length(object));
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
    {
        FunctionDocumentation::Description description = R"(
    Returns the perimeter of the object. For a `LineString` or a `MultiLineString`, returns its length.
        )";
        FunctionDocumentation::Syntax syntax = "perimeterCartesian(object)";
        FunctionDocumentation::Arguments arguments = {
            {"object", "geometry object", {"Variant"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "Returns the perimeter of the object.",
            {"Float64"}
        };
        FunctionDocumentation::Examples examples;

        FunctionDocumentation::IntroducedIn introduced_in = {25, 12};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionPerimeter<CartesianPoint>>(documentation);
    }
    {
            FunctionDocumentation::Description description = R"(
    Returns the perimeter of the object. For a `LineString` or a `MultiLineString`, returns its length.
        )";
        FunctionDocumentation::Syntax syntax = "perimeterSpherical(object)";
        FunctionDocumentation::Arguments arguments = {
            {"object", "geometry object", {"Variant"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "Returns the perimeter on a sphere of radius 1, in radians (coordinates are longitude and latitude in degrees). Multiply by the radius of the sphere to get a length, for example by 6371 for kilometers on the Earth.",
            {"Float64"}
        };
        FunctionDocumentation::Examples examples;

        FunctionDocumentation::IntroducedIn introduced_in = {25, 12};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};


        factory.registerFunction<FunctionPerimeter<SphericalPoint>>(documentation);
    }
}

}
