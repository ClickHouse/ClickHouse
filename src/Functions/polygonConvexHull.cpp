#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>


#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// `boost::geometry::convex_hull` takes only the exterior rings of polygons into account,
/// but it checks for emptiness with `boost::geometry::is_empty`, which also looks at the interior rings.
/// For a polygon with an empty exterior ring and a non-empty interior ring it reads uninitialized points.
/// Returns true if there are no points for the convex hull to be built from.
template <typename Geometry>
bool hasNoPointsForConvexHull(const Geometry & geometry)
{
    if constexpr (std::is_same_v<boost::geometry::tag_t<Geometry>, boost::geometry::polygon_tag>)
        return geometry.outer().empty();
    else if constexpr (std::is_same_v<boost::geometry::tag_t<Geometry>, boost::geometry::multi_polygon_tag>)
        return std::ranges::all_of(geometry, [](const auto & polygon) { return polygon.outer().empty(); });
    else
        return boost::geometry::is_empty(geometry);
}

/// The rectangle of the smallest area that contains a convex polygon, found with rotating calipers:
/// one of its sides lies on an edge of the polygon, so it is enough to try every edge, and the vertices
/// that are the farthest along and across an edge only move forward when going to the next edge.
template <typename Point>
Polygon<Point> minimumRotatedRectangle(const Polygon<Point> & convex_hull)
{
    using boost::geometry::get;
    auto equals = [](const Point & a, const Point & b) { return get<0>(a) == get<0>(b) && get<1>(a) == get<1>(b); };

    /// The vertices without repetitions and without the closing point.
    std::vector<Point> points;
    for (const auto & point : convex_hull.outer())
        if (points.empty() || !equals(point, points.back()))
            points.push_back(point);
    if (points.size() > 1 && equals(points.front(), points.back()))
        points.pop_back();

    if (points.empty())
        return convex_hull;

    Polygon<Point> rectangle;
    if (points.size() == 1)
    {
        rectangle.outer().assign(5, points.front());
        return rectangle;
    }

    const size_t n = points.size();
    auto next = [n](size_t i) { return i + 1 == n ? 0 : i + 1; };
    auto x = [&](size_t i) { return get<0>(points[i]); };
    auto y = [&](size_t i) { return get<1>(points[i]); };

    /// The calipers go counterclockwise, so that the inside of the polygon is to the left of every edge.
    /// The area is relative to the first vertex, so that it does not cancel out for coordinates far from the origin.
    Float64 doubled_signed_area = 0;
    for (size_t i = 0; i < n; ++i)
        doubled_signed_area += (x(i) - x(0)) * (y(next(i)) - y(0)) - (x(next(i)) - x(0)) * (y(i) - y(0));
    if (doubled_signed_area < 0)
        std::reverse(points.begin(), points.end());

    Float64 min_area = std::numeric_limits<Float64>::infinity();
    std::array<Point, 4> corners;
    size_t right = 1; /// The farthest vertex along the edge.
    size_t top = 0; /// The farthest vertex across the edge.
    size_t left = 0; /// The farthest vertex back along the edge.
    for (size_t i = 0; i < n; ++i)
    {
        /// The coordinates along and across the edge are in the units of its length, without a square root,
        /// which keeps the rounding small. The squares overflow for coordinates beyond 1e154.
        const Float64 ex = x(next(i)) - x(i);
        const Float64 ey = y(next(i)) - y(i);
        const Float64 squared_length = ex * ex + ey * ey;
        auto along = [&](size_t j) { return ((x(j) - x(i)) * ex + (y(j) - y(i)) * ey) / squared_length; };
        auto across = [&](size_t j) { return ((y(j) - y(i)) * ex - (x(j) - x(i)) * ey) / squared_length; };

        while (along(next(right)) > along(right))
            right = next(right);
        if (i == 0)
            top = right;
        while (across(next(top)) > across(top))
            top = next(top);
        if (i == 0)
            left = top;
        while (along(next(left)) < along(left))
            left = next(left);

        const Float64 min_along = along(left);
        const Float64 max_along = along(right);
        const Float64 height = across(top);
        const Float64 area = (max_along - min_along) * height * squared_length;
        /// The first edge is taken even if the area overflows to infinity.
        if (i == 0 || area < min_area)
        {
            min_area = area;
            auto corner = [&](Float64 a, Float64 c) { return Point(x(i) + a * ex - c * ey, y(i) + a * ey + c * ex); };
            /// Clockwise, as the rings made by `boost::geometry::convex_hull`.
            corners = {corner(min_along, 0), corner(min_along, height), corner(max_along, height), corner(max_along, 0)};
        }
    }

    /// Start from the leftmost corner, as `boost::geometry::convex_hull` does.
    std::rotate(corners.begin(), std::ranges::min_element(corners, {}, [](const Point & p) { return std::pair(get<0>(p), get<1>(p)); }), corners.end());
    rectangle.outer().assign(corners.begin(), corners.end());
    rectangle.outer().push_back(corners.front());
    return rectangle;
}

/// `polygonConvexHullCartesian`, or `polygonMinimumRotatedRectangleCartesian` if `minimum_rotated_rectangle` is true.
template <typename Point, bool minimum_rotated_rectangle>
class FunctionPolygonConvexHull final : public IFunction
{
public:
    static const char * name;

    explicit FunctionPolygonConvexHull() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonConvexHull>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    String getSignatureString() const override
    {
        return "(Any) -> Polygon";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        PolygonSerializer<Point> serializer;

        callOnGeometryDataType<Point>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            if constexpr (std::is_same_v<Converter, ColumnToPointsConverter<Point>>)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of function {} must not be a Point", getName());
            else
            {
                auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    Polygon<Point> convex_hull{};
                    if (!hasNoPointsForConvexHull(geometries[i]))
                        boost::geometry::convex_hull(geometries[i], convex_hull);
                    if constexpr (minimum_rotated_rectangle)
                        serializer.add(minimumRotatedRectangle(convex_hull));
                    else
                        serializer.add(convex_hull);
                }
            }
        }
        );

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonConvexHull<CartesianPoint, false>::name = "polygonConvexHullCartesian";

template <>
const char * FunctionPolygonConvexHull<CartesianPoint, true>::name = "polygonMinimumRotatedRectangleCartesian";

}

REGISTER_FUNCTION(PolygonConvexHull)
{
    FunctionDocumentation::Description description = R"(
Calculates a convex hull. [Reference](https://www.boost.org/doc/libs/1_61_0/libs/geometry/doc/html/geometry/reference/algorithms/convex_hull.html)

Coordinates are in Cartesian coordinate system.
    )";
    FunctionDocumentation::Syntax syntax = "polygonConvexHullCartesian(multipolygon)";
    FunctionDocumentation::Arguments arguments = {
        {"multipolygon", "A MultiPolygon value.", {"MultiPolygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the convex hull as a Polygon.", {"Polygon"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Conve hull example",
        R"(
SELECT wkt(polygonConvexHullCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.), (2., 3.)]]]))
        )",
        R"(
POLYGON((0 0,0 5,5 5,5 0,0 0))
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPolygonConvexHull<CartesianPoint, false>>(function_documentation);

    {
        FunctionDocumentation::Description rectangle_description = R"(
Returns the rectangle of the smallest area that contains a geometry, which can be rotated (the oriented envelope),
like `ST_OrientedEnvelope` in PostGIS and `ST_MinimumRotatedRectangle` in DuckDB.
It is found with rotating calipers over the convex hull, so the function takes the same arguments as `polygonConvexHullCartesian`.
For collinear points the rectangle has zero width, for a single point all its corners are that point, and without points the ring is empty.

Coordinates are in Cartesian coordinate system.
        )";
        FunctionDocumentation::Syntax rectangle_syntax = "polygonMinimumRotatedRectangleCartesian(geometry)";
        FunctionDocumentation::Arguments rectangle_arguments = {
            {"geometry", "A geometry other than a point.", {"MultiPolygon", "Polygon", "Ring", "MultiLineString", "LineString", "MultiPoint", "Geometry"}}
        };
        FunctionDocumentation::ReturnedValue rectangle_returned_value = {
            "Returns the rectangle as a Polygon with one ring: the four corners clockwise, and the first corner again.", {"Polygon"}};
        FunctionDocumentation::Examples rectangle_examples =
        {
        {
            "Rotated rectangle around points",
            R"(
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (4., 3.), (2., 3.), (-1.5, 2.), (1., 1.5)]::MultiPoint)
            )",
            R"(
[[(-1.5,2),(2.5,5),(4,3),(0,0),(-1.5,2)]]
            )"
        }
        };
        FunctionDocumentation::IntroducedIn rectangle_introduced_in = {26, 11};
        FunctionDocumentation rectangle_documentation = {
            rectangle_description, rectangle_syntax, rectangle_arguments, {}, rectangle_returned_value, rectangle_examples, rectangle_introduced_in, category};

        factory.registerFunction<FunctionPolygonConvexHull<CartesianPoint, true>>(rectangle_documentation);
    }
}

}
