#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>


#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <memory>
#include <utility>

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

/// Multiplies the x and the y coordinates by powers of two, which is exact unless a result is subnormal.
template <typename Point, typename Geometry>
void scaleByPowersOfTwo(Geometry & geometry, int x_exponent, int y_exponent)
{
    boost::geometry::for_each_point(geometry, [x_exponent, y_exponent](Point & point)
    {
        point = Point(std::ldexp(boost::geometry::get<0>(point), x_exponent), std::ldexp(boost::geometry::get<1>(point), y_exponent));
    });
}

int exponentOf(Float64 value)
{
    return value == 0 || std::isinf(value) ? 0 : std::ilogb(value);
}

/// The rotating calipers of `convexHullToMinimumRotatedRectangle`, on a clockwise convex ring
/// with coordinates below 2^401 in absolute value.
template <typename Point, typename Ring>
void replaceWithMinimumRotatedRectangle(Ring & points)
{
    using boost::geometry::get;
    auto equals = [](const Point & a, const Point & b) { return get<0>(a) == get<0>(b) && get<1>(a) == get<1>(b); };

    /// The vertices without repetitions and without the closing point.
    points.erase(std::unique(points.begin(), points.end(), equals), points.end());
    if (points.size() > 1 && equals(points.front(), points.back()))
        points.pop_back();

    if (points.empty())
        return;

    if (points.size() == 1)
    {
        const Point point = points.front();
        points.assign(5, point);
        return;
    }

    /// The calipers go counterclockwise, so that the inside of the polygon is to the left of every edge.
    std::reverse(points.begin(), points.end());

    const size_t n = points.size();
    auto next = [n](size_t i) { return i + 1 == n ? 0 : i + 1; };
    auto x = [&](size_t i) { return get<0>(points[i]); };
    auto y = [&](size_t i) { return get<1>(points[i]); };

    /// Brings back the values multiplied by an edge scaled to 2^600, see below. It is exact unless a result is subnormal.
    constexpr Float64 down = 0x1p-600;

    Float64 min_area = std::numeric_limits<Float64>::infinity();
    std::array<Point, 4> corners{};
    size_t right = 0; /// The farthest vertex along the edge.
    size_t top = 0; /// The farthest vertex across the edge.
    size_t left = 0; /// The farthest vertex back along the edge.
    for (size_t i = 0; i < n; ++i)
    {
        /// Only the direction of the edge matters, so it is multiplied by a power of two that brings its larger coordinate
        /// to 2^600. Then its products with the differences of coordinates neither overflow nor underflow,
        /// even for a short edge, or for a long edge with a tiny difference in the other coordinate.
        const Float64 edge_x = x(next(i)) - x(i);
        const Float64 edge_y = y(next(i)) - y(i);
        const int edge_scale = 600 - std::ilogb(std::max(std::abs(edge_x), std::abs(edge_y)));
        const Float64 ex = std::ldexp(edge_x, edge_scale);
        const Float64 ey = std::ldexp(edge_y, edge_scale);
        /// Everything along and across the edge is multiplied by its length, which needs no square root,
        /// and that keeps the rounding small.
        const Float64 squared_length = (ex * down) * (ex * down) + (ey * down) * (ey * down);
        auto along = [&](Float64 dx, Float64 dy) { return dx * ex + dy * ey; };
        auto across = [&](Float64 dx, Float64 dy) { return dy * ex - dx * ey; };
        auto along_step = [&](size_t j) { return along(x(next(j)) - x(j), y(next(j)) - y(j)); };
        auto across_step = [&](size_t j) { return across(x(next(j)) - x(j), y(next(j)) - y(j)); };
        auto along_from_edge = [&](size_t j) { return along(x(j) - x(i), y(j) - y(i)) * down; };
        auto across_from_edge = [&](size_t j) { return across(x(j) - x(i), y(j) - y(i)) * down; };
        auto steps_from_edge = [&](size_t j) { return (j + n - i) % n; };

        /// Go until the next edge turns by 90, 180 and 270 degrees from the current one. The directions of the edges
        /// are compared, not the positions of the vertices, because a short edge can round to no change of position.
        if (right == i)
            right = next(i);
        while (right != i && along_step(right) > 0)
            right = next(right);
        /// `top` and `left` continue from `right` if they are behind it, as at the start, or at the start of the edge.
        /// `left` does not continue from `top`: on a nearly flat polygon rounding can carry `top` around to the start of the edge.
        if (steps_from_edge(top) < steps_from_edge(right))
            top = right;
        while (top != i && across_step(top) > 0)
            top = next(top);
        if (steps_from_edge(left) < steps_from_edge(right))
            left = right;
        /// An edge turned by exactly 90 degrees goes neither along nor back, and `left` passes it.
        while (left != i && (along_step(left) < 0 || (along_step(left) == 0 && across_step(left) > 0)))
            left = next(left);

        const Float64 area = (along_from_edge(right) - along_from_edge(left)) / squared_length * across_from_edge(top);
        if (area < min_area)
        {
            min_area = area;
            const Float64 min_along = along_from_edge(left) / squared_length;
            const Float64 max_along = along_from_edge(right) / squared_length;
            const Float64 height = across_from_edge(top) / squared_length;
            auto corner = [&](Float64 a, Float64 c) { return Point(x(i) + (a * ex - c * ey) * down, y(i) + (a * ey + c * ex) * down); };
            /// Clockwise, as the rings made by `boost::geometry::convex_hull`.
            corners = {corner(min_along, 0), corner(min_along, height), corner(max_along, height), corner(max_along, 0)};
        }
    }

    /// Start from the leftmost corner, as `boost::geometry::convex_hull` does.
    std::rotate(corners.begin(), std::ranges::min_element(corners, {}, [](const Point & p) { return std::pair(get<0>(p), get<1>(p)); }), corners.end());
    points.assign(corners.begin(), corners.end());
    points.push_back(corners.front());
}

/// Replaces a convex polygon with the rectangle of the smallest area that contains it, found with rotating calipers:
/// one of its sides lies on an edge of the polygon, so it is enough to try every edge, and the vertices
/// that are the farthest along and across an edge only move forward when going to the next edge.
/// It works in place on the ring of the polygon, which is memory tracked, without allocating anything else.
/// The complexity is linear in the number of vertices: none of the three vertices passes the current edge,
/// so each goes around the polygon at most twice, even with rounding errors.
template <typename Point>
void convexHullToMinimumRotatedRectangle(Polygon<Point> & polygon)
{
    /// Bring the largest coordinate between 2^-400 and 2^400 and back. Coordinates smaller than the largest one
    /// by more than 2^1470 can round to zero.
    Float64 max_abs = 0;
    boost::geometry::for_each_point(polygon, [&](const Point & point)
    {
        max_abs = std::max({max_abs, std::abs(boost::geometry::get<0>(point)), std::abs(boost::geometry::get<1>(point))});
    });
    const int scale = exponentOf(max_abs) - std::clamp(exponentOf(max_abs), -400, 400);
    if (scale != 0)
        scaleByPowersOfTwo<Point>(polygon, -scale, -scale);

    replaceWithMinimumRotatedRectangle<Point>(polygon.outer());

    if (scale != 0)
        scaleByPowersOfTwo<Point>(polygon, scale, scale);
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

                /// Reused between rows to keep the memory of its ring.
                Polygon<Point> result;

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    /// Boost decides the side of a point exactly from the products of differences of x and y coordinates,
                    /// unless they overflow or underflow. Multiplying the x and the y coordinates by positive factors keeps
                    /// the convex hull the same, so then each of them is scaled to bring its largest absolute value to 2^500.
                    /// It is exact unless there are also values smaller than the largest one by more than 2^1500.
                    Float64 max_x = 0;
                    Float64 max_y = 0;
                    Float64 min_x = std::numeric_limits<Float64>::infinity(); /// The smallest nonzero absolute values.
                    Float64 min_y = std::numeric_limits<Float64>::infinity();
                    boost::geometry::for_each_point(geometries[i], [&](const Point & point)
                    {
                        const Float64 abs_x = std::abs(boost::geometry::get<0>(point));
                        const Float64 abs_y = std::abs(boost::geometry::get<1>(point));
                        max_x = std::max(max_x, abs_x);
                        max_y = std::max(max_y, abs_y);
                        if (abs_x != 0)
                            min_x = std::min(min_x, abs_x);
                        if (abs_y != 0)
                            min_y = std::min(min_y, abs_y);
                    });
                    /// A nonzero difference of coordinates is at least 2^-52 of the smaller of them, or of the nonzero one.
                    const bool scaled = exponentOf(max_x) + exponentOf(max_y) > 1000 || exponentOf(min_x) + exponentOf(min_y) < -800;
                    const int x_scale = scaled ? 500 - exponentOf(max_x) : 0;
                    const int y_scale = scaled ? 500 - exponentOf(max_y) : 0;
                    if (scaled)
                        scaleByPowersOfTwo<Point>(geometries[i], x_scale, y_scale);

                    result.clear();
                    if (!hasNoPointsForConvexHull(geometries[i]))
                        boost::geometry::convex_hull(geometries[i], result);

                    if (scaled)
                        scaleByPowersOfTwo<Point>(result, -x_scale, -y_scale);
                    if constexpr (minimum_rotated_rectangle)
                        convexHullToMinimumRotatedRectangle(result);

                    serializer.add(result);
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
