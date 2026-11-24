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
    /// A multiplication by a power of two rounds the same as `std::ldexp`, and it is much faster.
    auto is_normal_exponent = [](int exponent) { return exponent >= -1022 && exponent <= 1023; };
    if (is_normal_exponent(x_exponent) && is_normal_exponent(y_exponent))
    {
        const Float64 x_factor = std::ldexp(1.0, x_exponent);
        const Float64 y_factor = std::ldexp(1.0, y_exponent);
        boost::geometry::for_each_point(geometry, [x_factor, y_factor](Point & point)
        {
            point = Point(boost::geometry::get<0>(point) * x_factor, boost::geometry::get<1>(point) * y_factor);
        });
        return;
    }
    boost::geometry::for_each_point(geometry, [x_exponent, y_exponent](Point & point)
    {
        point = Point(std::ldexp(boost::geometry::get<0>(point), x_exponent), std::ldexp(boost::geometry::get<1>(point), y_exponent));
    });
}

int exponentOf(Float64 value)
{
    return value == 0 || std::isinf(value) ? 0 : std::ilogb(value);
}

/// The powers of two for the x and the y coordinates before the convex hull, from the smallest nonzero
/// and the largest absolute values on each axis. The products of the differences of the scaled coordinates must neither
/// overflow nor underflow. Bringing the largest value of each axis to 2^500 does that, so these powers are preferred,
/// but they are moved, if possible, so that no coordinate becomes subnormal, where it would be rounded.
/// If that is not possible together with the bound for underflow, which is for the smallest differences on both axes at once,
/// the bound is dropped: a rounded coordinate is a sure error, and a product that underflows rarely decides a side.
/// An axis that spans more than `Float64` can hold after scaling keeps only the bound for overflow, and its smallest values round.
std::pair<int, int> hullScales(Float64 min_x, Float64 max_x, Float64 min_y, Float64 max_y)
{
    const int x_high = 1020 - exponentOf(max_x);
    const int y_high = 1020 - exponentOf(max_y);
    const int x_low = std::min(-1022 - exponentOf(min_x), x_high);
    const int y_low = std::min(-1022 - exponentOf(min_y), y_high);
    const int sum_high = std::min(1000 - exponentOf(max_x) - exponentOf(max_y), x_high + y_high);
    int sum_low = std::max(-800 - exponentOf(min_x) - exponentOf(min_y), x_low + y_low);
    if (sum_low > sum_high)
        sum_low = x_low + y_low;

    int x_scale = 500 - exponentOf(max_x);
    int y_scale = 500 - exponentOf(max_y);
    if (sum_low <= sum_high)
    {
        const int sum = std::clamp(x_scale + y_scale, sum_low, sum_high);
        x_scale = std::clamp(x_scale, std::max(x_low, sum - y_high), std::min(x_high, sum - y_low));
        y_scale = sum - x_scale;
    }
    return {x_scale, y_scale};
}

/// A `Float64` with a separate exponent, so that sums and products of any coordinates neither overflow nor underflow.
class FloatWithExponent
{
public:
    explicit FloatWithExponent(Float64 value) : FloatWithExponent(value, 0) {}
    explicit operator Float64() const { return std::ldexp(mantissa, exponent); }

    friend FloatWithExponent operator*(FloatWithExponent a, FloatWithExponent b) { return {a.mantissa * b.mantissa, a.exponent + b.exponent}; }
    friend FloatWithExponent operator/(FloatWithExponent a, FloatWithExponent b) { return {a.mantissa / b.mantissa, a.exponent - b.exponent}; }
    friend FloatWithExponent operator+(FloatWithExponent a, FloatWithExponent b)
    {
        const int larger = std::max(a.exponent, b.exponent);
        return {std::ldexp(a.mantissa, a.exponent - larger) + std::ldexp(b.mantissa, b.exponent - larger), larger};
    }
    friend FloatWithExponent operator-(FloatWithExponent a, FloatWithExponent b) { return a + FloatWithExponent(-b.mantissa, b.exponent); }
    friend bool operator<(FloatWithExponent a, FloatWithExponent b) { return (a - b).mantissa < 0; }
    friend bool operator>(FloatWithExponent a, FloatWithExponent b) { return (a - b).mantissa > 0; }
    friend bool operator==(FloatWithExponent a, FloatWithExponent b) { return (a - b).mantissa == 0; }

private:
    /// Zero gets the lowest exponent, so that adding it does not round the other number.
    static constexpr int zero_exponent = std::numeric_limits<int>::min() / 4;

    FloatWithExponent(Float64 value, int extra_exponent)
        : mantissa(std::frexp(value, &exponent))
    {
        exponent = mantissa == 0 ? zero_exponent : exponent + extra_exponent;
    }

    Float64 mantissa;
    int exponent;
};

/// The rotating calipers of `convexHullToMinimumRotatedRectangle`, on a clockwise convex ring.
/// With `Float64`, the largest absolute value of a coordinate must be around 2^400.
/// `max_coordinate` is the largest absolute value of a corner that stays finite when the ring is scaled back.
template <typename Point, typename Number, typename Ring>
void replaceWithMinimumRotatedRectangle(Ring & points, Float64 max_coordinate)
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
    auto x = [&](size_t i) { return Number(get<0>(points[i])); };
    auto y = [&](size_t i) { return Number(get<1>(points[i])); };

    /// Brings back the values multiplied by an edge scaled to 2^600, see below. It is exact unless a result is subnormal.
    constexpr bool scale_edges = std::is_same_v<Number, Float64>;
    const Number down(scale_edges ? 0x1p-600 : 1.0);
    const Number zero(0.0);

    Number min_area = zero;
    std::array<Point, 4> corners{};
    bool corners_are_finite = false;
    /// Of the rectangles with the same area up to rounding, one whose corners fit in `Float64` is taken.
    const Number almost_one(1 + 0x1p-40);
    size_t right = 0; /// The farthest vertex along the edge.
    size_t top = 0; /// The farthest vertex across the edge.
    size_t left = 0; /// The farthest vertex back along the edge.
    for (size_t i = 0; i < n; ++i)
    {
        /// Only the direction of the edge matters, so it is multiplied by a power of two that brings its larger coordinate
        /// to 2^600. Then its products with the differences of coordinates neither overflow nor underflow,
        /// even for a short edge, or for a long edge with a tiny difference in the other coordinate.
        /// `FloatWithExponent` needs no scaling.
        Number ex = x(next(i)) - x(i);
        Number ey = y(next(i)) - y(i);
        if constexpr (scale_edges)
        {
            const int edge_scale = 600 - std::ilogb(std::max(std::abs(ex), std::abs(ey)));
            ex = std::ldexp(ex, edge_scale);
            ey = std::ldexp(ey, edge_scale);
        }
        /// Everything along and across the edge is multiplied by its length, which needs no square root,
        /// and that keeps the rounding small.
        const Number squared_length = (ex * down) * (ex * down) + (ey * down) * (ey * down);
        auto along = [&](Number dx, Number dy) { return dx * ex + dy * ey; };
        auto across = [&](Number dx, Number dy) { return dy * ex - dx * ey; };
        auto along_step = [&](size_t j) { return along(x(next(j)) - x(j), y(next(j)) - y(j)); };
        auto across_step = [&](size_t j) { return across(x(next(j)) - x(j), y(next(j)) - y(j)); };
        auto along_from_edge = [&](size_t j) { return along(x(j) - x(i), y(j) - y(i)) * down; };
        auto across_from_edge = [&](size_t j) { return across(x(j) - x(i), y(j) - y(i)) * down; };
        auto steps_from_edge = [&](size_t j) { return (j + n - i) % n; };

        /// Go until the next edge turns by 90, 180 and 270 degrees from the current one. The directions of the edges
        /// are compared, not the positions of the vertices, because a short edge can round to no change of position.
        if (right == i)
            right = next(i);
        while (right != i && along_step(right) > zero)
            right = next(right);
        /// `top` and `left` continue from `right` if they are behind it, as at the start, or at the start of the edge.
        /// `left` does not continue from `top`: on a nearly flat polygon rounding can carry `top` around to the start of the edge.
        if (steps_from_edge(top) < steps_from_edge(right))
            top = right;
        while (top != i && across_step(top) > zero)
            top = next(top);
        if (steps_from_edge(left) < steps_from_edge(right))
            left = right;
        /// An edge turned by exactly 90 degrees goes neither along nor back, and `left` passes it.
        while (left != i && (along_step(left) < zero || (along_step(left) == zero && across_step(left) > zero)))
            left = next(left);

        const Number area = (along_from_edge(right) - along_from_edge(left)) / squared_length * across_from_edge(top);
        if (i == 0 || area < min_area || (!corners_are_finite && !(min_area * almost_one < area)))
        {
            min_area = area;
            const Number min_along = along_from_edge(left) / squared_length;
            const Number max_along = along_from_edge(right) / squared_length;
            const Number height = across_from_edge(top) / squared_length;
            /// The corner at `a` along the edge from the vertex `along_vertex` and at `c` across it from the vertex `across_vertex`.
            /// If the edge is parallel to an axis, the coordinates of these vertices are the exact ones.
            auto corner = [&](size_t along_vertex, Number a, size_t across_vertex, Number c)
            {
                if (ey == zero)
                    return Point(get<0>(points[along_vertex]), get<1>(points[across_vertex]));
                if (ex == zero)
                    return Point(get<0>(points[across_vertex]), get<1>(points[along_vertex]));
                return Point(Float64(x(i) + (a * ex - c * ey) * down), Float64(y(i) + (a * ey + c * ex) * down));
            };
            /// Clockwise, as the rings made by `boost::geometry::convex_hull`.
            corners = {corner(left, min_along, i, zero), corner(left, min_along, top, height),
                corner(right, max_along, top, height), corner(right, max_along, i, zero)};
            corners_are_finite = std::ranges::all_of(corners, [&](const Point & p)
            {
                return std::abs(get<0>(p)) <= max_coordinate && std::abs(get<1>(p)) <= max_coordinate;
            });
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
    /// Bring the largest coordinate to 2^400 and back, which changes no rounding. Then no product of the calipers overflows,
    /// and the area of a rectangle that is not flat does not underflow, even if one axis is much smaller than the other.
    Float64 max_abs = 0;
    Float64 min_abs = std::numeric_limits<Float64>::infinity(); /// The smallest nonzero absolute value.
    boost::geometry::for_each_point(polygon, [&](const Point & point)
    {
        for (const Float64 value : {std::abs(boost::geometry::get<0>(point)), std::abs(boost::geometry::get<1>(point))})
        {
            max_abs = std::max(max_abs, value);
            if (value != 0)
                min_abs = std::min(min_abs, value);
        }
    });
    const int scale = max_abs == 0 ? 0 : exponentOf(max_abs) - 400;

    /// Scaling down would round the coordinates that become subnormal, so then it computes with separate exponents.
    if (scale > 0 && exponentOf(min_abs) - scale < -1022)
    {
        replaceWithMinimumRotatedRectangle<Point, FloatWithExponent>(polygon.outer(), std::numeric_limits<Float64>::max());
        return;
    }

    if (scale != 0)
        scaleByPowersOfTwo<Point>(polygon, -scale, -scale);

    replaceWithMinimumRotatedRectangle<Point, Float64>(polygon.outer(), std::ldexp(std::numeric_limits<Float64>::max(), -scale));

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
                    /// the convex hull the same, so then each of them is scaled by a power of two, see `hullScales`.
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
                    /// A difference of coordinates near the largest `Float64` overflows by itself.
                    const bool scaled = exponentOf(max_x) + exponentOf(max_y) > 1000 || exponentOf(min_x) + exponentOf(min_y) < -800
                        || std::max(exponentOf(max_x), exponentOf(max_y)) > 1020;
                    const auto [x_scale, y_scale] = scaled ? hullScales(min_x, max_x, min_y, max_y) : std::pair(0, 0);
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
