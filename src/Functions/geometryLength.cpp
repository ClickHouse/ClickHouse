#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/geometry.h>

#include <boost/geometry/algorithms/for_each.hpp>
#include <boost/geometry/algorithms/length.hpp>
#include <boost/geometry/strategies/geographic/parameters.hpp>
#include <boost/geometry/strategies/length/geographic.hpp>

#include <cmath>
#include <limits>

namespace DB
{

namespace
{

/// Longitude and latitude in degrees on the WGS84 spheroid (the model of the Earth used by GPS),
/// which is the default spheroid of Boost.Geometry.
using GeographicPoint = boost::geometry::model::point<Float64, 2, boost::geometry::cs::geographic<boost::geometry::degree>>;

template <typename Point, typename Geometry>
Float64 linearLength(const Geometry & geometry)
{
    /// Karney's algorithm for geodesics, as in GeographicLib: accurate for any pair of points, including nearly antipodal ones,
    /// for which Vincenty's formulae may not converge.
    if constexpr (std::is_same_v<Point, GeographicPoint>)
    {
        /// A NaN or infinite coordinate, or a latitude outside [-90, 90], has no position on the spheroid, so the length is NaN,
        /// as in GeographicLib. Karney's algorithm must not get a non-finite coordinate: it asserts that the coordinates are finite.
        bool all_valid = true;
        bool needs_wrap = false;
        boost::geometry::for_each_point(geometry, [&](const GeographicPoint & point)
        {
            Float64 longitude = boost::geometry::get<0>(point);
            all_valid = all_valid && std::isfinite(longitude) && std::abs(boost::geometry::get<1>(point)) <= 90;
            needs_wrap = needs_wrap || std::abs(longitude) > 180;
        });
        if (!all_valid)
            return std::numeric_limits<Float64>::quiet_NaN();

        const auto strategy = boost::geometry::strategies::length::geographic<boost::geometry::strategy::karney>();
        if (!needs_wrap)
            return static_cast<Float64>(boost::geometry::length(geometry, strategy));

        /// Boost converts longitudes to radians and back, which loses precision for large values,
        /// so bring them to [-180, 180] first: `std::remainder` is exact.
        Geometry wrapped = geometry;
        boost::geometry::for_each_point(wrapped, [](GeographicPoint & point)
        {
            boost::geometry::set<0>(point, std::remainder(boost::geometry::get<0>(point), 360.0));
        });
        return static_cast<Float64>(boost::geometry::length(wrapped, strategy));
    }
    else
        return static_cast<Float64>(boost::geometry::length(geometry));
}

/// The length of the lines in a geometry, like `ST_Length`: points and areas have no length.
template <typename Point>
struct LengthCalculator
{
    Float64 operator()(const Point &) { return 0; }

    Float64 operator()(const MultiPoint<Point> &) { return 0; }

    Float64 operator()(const LineString<Point> & object) { return linearLength<Point>(object); }

    Float64 operator()(const MultiLineString<Point> & object) { return linearLength<Point>(object); }

    /// An array of points without a type name is a `Ring`, so measure a ring as the line through its points,
    /// as `LinearRing` in OGC. `boost::geometry::length` treats a ring as an area and returns 0 for it.
    Float64 operator()(const Ring<Point> & object)
    {
        return linearLength<Point>(LineString<Point>(object.begin(), object.end()));
    }

    Float64 operator()(const Polygon<Point> &) { return 0; }

    Float64 operator()(const MultiPolygon<Point> &) { return 0; }
};

template <typename Point>
using FunctionGeometryLength = FunctionGeometry<Point, LengthCalculator<Point>>;

}

template <>
const char * FunctionGeometryLength<CartesianPoint>::name = "lengthCartesian";

template <>
const char * FunctionGeometryLength<SphericalPoint>::name = "lengthSpherical";

template <>
const char * FunctionGeometryLength<GeographicPoint>::name = "lengthSpheroid";

REGISTER_FUNCTION(GeometryLength)
{
    const String what_is_measured = R"(
For a `MultiLineString`, returns the total length of its lines.
A `Ring` is measured as the line through its points, and so is an array of points without a type name, which has the type `Ring`.
Points, `Polygon` and `MultiPolygon` have no length, so the function returns `0` for them, like `ST_Length`: use the `perimeter*` functions for polygons.
Cast an array of arrays of points to `MultiLineString`: without a type name, it has the type `Polygon`.
)";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "A line or any other geometry.", {"Geometry", "LineString", "MultiLineString", "Ring", "Point", "MultiPoint", "Polygon", "MultiPolygon"}}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;

    {
        FunctionDocumentation::Description description = R"(
Returns the length of a line on a plane: the sum of the Euclidean distances between consecutive points, in the units of the coordinates.
)" + what_is_measured;
        FunctionDocumentation::Syntax syntax = "lengthCartesian(geometry)";
        FunctionDocumentation::ReturnedValue returned_value = {"The length of the lines, in the units of the coordinates.", {"Float64"}};
        FunctionDocumentation::Examples examples = {
            {
                "Length of a line",
                "SELECT lengthCartesian([(0., 0.), (3., 4.), (3., 10.)]::LineString) AS length",
                R"(
┌─length─┐
│     11 │
└────────┘
                )"
            },
            {
                "Length of several lines",
                "SELECT lengthCartesian(readWKT('MULTILINESTRING ((0 0, 0 2), (5 5, 5 6))')) AS length",
                R"(
┌─length─┐
│      3 │
└────────┘
                )"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionGeometryLength<CartesianPoint>>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Returns the length of a line on a sphere, with points given as (longitude, latitude) in degrees.
The length is an angle in radians, which is the length on a sphere of radius 1, like the result of `perimeterSpherical`.
Multiply it by the mean radius of the Earth, 6371008.8 meters, for an approximate length in meters, or use `lengthSpheroid` for an accurate one.
)" + what_is_measured;
        FunctionDocumentation::Syntax syntax = "lengthSpherical(geometry)";
        FunctionDocumentation::ReturnedValue returned_value = {"The length of the lines, in radians.", {"Float64"}};
        FunctionDocumentation::Examples examples = {
            {
                "One degree along the equator",
                "SELECT lengthSpherical([(0., 0.), (1., 0.)]::LineString) AS length",
                R"(
┌───────────────length─┐
│ 0.017453292519943292 │
└──────────────────────┘
                )"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionGeometryLength<SphericalPoint>>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Returns the length of a line in meters on the WGS84 spheroid, the model of the Earth used by GPS, with points given as (longitude, latitude) in degrees.
Each segment is the shortest path between its points on the spheroid (a geodesic), calculated with Karney's algorithm, as in GeographicLib.
For coordinates stored as (latitude, longitude), such as the input of `ST_Length_Spheroid` in DuckDB, swap them first with `flipCoordinates`:
without it, the result is the length of a different line.
If a coordinate is NaN or infinite, or a latitude is outside [-90, 90], the result is NaN.
)" + what_is_measured;
        FunctionDocumentation::Syntax syntax = "lengthSpheroid(geometry)";
        FunctionDocumentation::ReturnedValue returned_value = {"The length of the lines, in meters.", {"Float64"}};
        FunctionDocumentation::Examples examples = {
            {
                "One degree along the equator",
                "SELECT lengthSpheroid([(0., 0.), (1., 0.)]::LineString) AS length",
                R"(
┌─────────────length─┐
│ 111319.49079327357 │
└────────────────────┘
                )"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionGeometryLength<GeographicPoint>>(documentation);
    }

    factory.registerAlias("ST_Length", FunctionGeometryLength<CartesianPoint>::name);
}

}
