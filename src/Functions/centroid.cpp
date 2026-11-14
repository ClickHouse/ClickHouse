#include "config.h"

#include <Functions/FunctionFactory.h>
#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <Columns/ColumnVariant.h>

#include <base/EnumReflection.h>

#include <cfloat>
#include <cmath>
#include <limits>
#include <numbers>
#include <utility>
#include <vector>

#if USE_S2_GEOMETRY
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wambiguous-reversed-operator"
#include <s2/s2latlng.h>
#include <s2/s2loop_measures.h>
#include <s2/s2measures.h>
#include <s2/s2point.h>
#include <s2/s2point_span.h>
#include <s2/s2polyline_measures.h>
#pragma clang diagnostic pop
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// As in JTS and GEOS, the centroid uses the highest dimension with a nonzero measure:
/// - the area-weighted centroid of the polygons;
/// - if their area is zero up to rounding (for example, every ring lies on a line), the length-weighted centroid of
///   the ring boundaries and the linestrings;
/// - if that length is zero too, the mean of the points, where a linestring or ring of zero length counts as its first
///   vertex.
/// An empty geometry has no centroid, so the result is NaN, like `POINT EMPTY` in WKB.

constexpr Float64 NaN = std::numeric_limits<Float64>::quiet_NaN();

/// The area and the moments of each ring are computed relative to its first vertex, to reduce cancellation in the cross
/// products, and then shifted to a common origin, the first point of the geometry. So the rounding error depends on the
/// size of each ring, but not on its distance from the other parts.
/// `boost::geometry::centroid` shifts the coordinates too, but is not used: it detects zero area with an absolute
/// epsilon, so it treats a tiny polygon as degenerate and returns its first vertex.
class CartesianCentroid
{
public:
    using Point = CartesianPoint;

    void reset()
    {
        sums = {};
        has_origin = false;
    }

    void addRing(const Ring<Point> & ring, bool is_hole)
    {
        const size_t n = ringVertexCount(ring);
        if (n == 0)
            return;
        setOrigin(ring.front());

        /// Shoelace formula relative to the first vertex: twice the signed area, and six times the first moments.
        const Point & ring_origin = ring.front();
        auto translate_to_ring = [&](const Point & point) -> std::pair<Float64, Float64>
        {
            return {point.x() - ring_origin.x(), point.y() - ring_origin.y()};
        };
        Float64 ring_area2 = 0;
        Float64 ring_moment_x = 0;
        Float64 ring_moment_y = 0;
        for (size_t i = 0; i < n; ++i)
        {
            const auto [x1, y1] = translate_to_ring(ring[i]);
            const auto [x2, y2] = translate_to_ring(ring[i + 1 == n ? 0 : i + 1]);
            const Float64 cross = x1 * y2 - x2 * y1;
            ring_area2 += cross;
            ring_moment_x += cross * (x1 + x2);
            ring_moment_y += cross * (y1 + y2);
            sums.area_magnitude += std::abs(x1 * y2) + std::abs(x2 * y1);
        }
        /// Whatever the winding, an outer ring adds its area, and a hole subtracts it.
        if ((ring_area2 < 0) != is_hole)
        {
            ring_area2 = -ring_area2;
            ring_moment_x = -ring_moment_x;
            ring_moment_y = -ring_moment_y;
        }
        /// Six times the first moments relative to the common origin: the area times six times the shift.
        const auto [shift_x, shift_y] = translate(ring_origin);
        ring_moment_x += 3 * shift_x * ring_area2;
        ring_moment_y += 3 * shift_y * ring_area2;
        sums.area2 += ring_area2;
        sums.area_moment_x += ring_moment_x;
        sums.area_moment_y += ring_moment_y;
        sums.area_terms += n;

        addPath(ring, n, /* closed */ true);
    }

    void addLineString(const LineString<Point> & linestring) { addPath(linestring, linestring.size(), /* closed */ false); }

    void addPoint(const Point & point)
    {
        setOrigin(point);
        const auto [x, y] = translate(point);
        sums.point_sum_x += x;
        sums.point_sum_y += y;
        ++sums.points;
    }

    Point get() const
    {
        /// The rounding error of twice the area is below (n + 4)·ε times the sum of the magnitudes of its products.
        if (std::abs(sums.area2) > static_cast<Float64>(sums.area_terms + 4) * DBL_EPSILON * sums.area_magnitude)
            return Point{sums.area_moment_x / (3 * sums.area2) + origin_x, sums.area_moment_y / (3 * sums.area2) + origin_y};
        if (sums.length > 0)
            return Point{sums.line_moment_x / sums.length + origin_x, sums.line_moment_y / sums.length + origin_y};
        if (sums.points > 0)
        {
            const auto points = static_cast<Float64>(sums.points);
            return Point{sums.point_sum_x / points + origin_x, sums.point_sum_y / points + origin_y};
        }
        return Point{NaN, NaN};
    }

private:
    void setOrigin(const Point & point)
    {
        if (has_origin)
            return;
        origin_x = point.x();
        origin_y = point.y();
        has_origin = true;
    }

    std::pair<Float64, Float64> translate(const Point & point) const { return {point.x() - origin_x, point.y() - origin_y}; }

    /// Adds the segments of the first `n` points of `path`. A path of zero length counts as its first point.
    template <typename Path>
    void addPath(const Path & path, size_t n, bool closed)
    {
        if (n == 0)
            return;
        setOrigin(path.front());

        Float64 path_length = 0;
        const size_t segments = closed ? n : n - 1;
        for (size_t i = 0; i < segments; ++i)
        {
            const Point & from = path[i];
            const Point & to = path[i + 1 == n ? 0 : i + 1];
            const auto [x1, y1] = translate(from);
            const auto [x2, y2] = translate(to);
            /// From the original coordinates, which are exact, unlike the shifted ones.
            const Float64 segment_length = std::hypot(to.x() - from.x(), to.y() - from.y());
            path_length += segment_length;
            sums.line_moment_x += 0.5 * segment_length * (x1 + x2);
            sums.line_moment_y += 0.5 * segment_length * (y1 + y2);
        }
        sums.length += path_length;
        if (path_length == 0)
            addPoint(path.front());
    }

    struct Sums
    {
        Float64 area2 = 0;
        Float64 area_moment_x = 0;
        Float64 area_moment_y = 0;
        Float64 area_magnitude = 0;
        size_t area_terms = 0;

        Float64 length = 0;
        Float64 line_moment_x = 0;
        Float64 line_moment_y = 0;

        Float64 point_sum_x = 0;
        Float64 point_sum_y = 0;
        size_t points = 0;
    };

    Sums sums;
    Float64 origin_x = 0;
    Float64 origin_y = 0;
    bool has_origin = false;
};

#if USE_S2_GEOMETRY

/// The centroid on the unit sphere. For each dimension, sums the results of `S2::GetCentroid` (the centroid scaled by
/// the area or the length) and normalizes the sum at the end.
class SphericalCentroid
{
public:
    using Point = SphericalPoint;

    void reset() { sums = {}; }

    void addRing(const Ring<Point> & ring, bool is_hole)
    {
        const size_t n = ringVertexCount(ring);
        if (n == 0)
            return;
        toS2Points(ring, n);
        if (n >= 3)
        {
            const S2PointLoopSpan loop(vertices.data(), n);
            /// The area of the region on the left of the loop, or minus the area of the region on its right, whichever is
            /// smaller in magnitude. This is `S2::GetSignedArea` without its check of the sign for a loop of nearly zero
            /// area. That check does not matter here, because the area and the centroid of such a loop are both nearly
            /// zero. It also has an assertion that fails for a loop going back and forth along a great circle, for example
            /// `[(90, 0), (45, 0), (30, 0), (60, 0)]`.
            const double signed_area = std::remainder(S2::GetSurfaceIntegralKahan(loop, S2::SignedArea), 4 * std::numbers::pi);
            const S2Point centroid = S2::GetCentroid(loop);
            const double area = std::abs(signed_area);
            /// The interior is the smaller of the two regions the ring bounds, whatever the winding, or the region on its
            /// right if the two are equal (`isHemisphere`). `S2::GetCentroid` is for the region on the left; the one for
            /// the region on the right is its negation.
            const bool interior_on_right = signed_area < 0 || isHemisphere(area, ringEdgeCount(ring, n));
            if (interior_on_right != is_hole)
                sums.area_centroid -= centroid;
            else
                sums.area_centroid += centroid;
            sums.area += is_hole ? -area : area;
            sums.area_magnitude += area;
            sums.area_error += S2::GetCurvatureMaxError(loop);
            ++sums.rings;
        }
        /// The boundary, as a closed polyline.
        vertices.push_back(vertices.front());
        addPolyline();
    }

    void addLineString(const LineString<Point> & linestring)
    {
        if (linestring.empty())
            return;
        toS2Points(linestring, linestring.size());
        addPolyline();
    }

    void addPoint(const Point & point)
    {
        sums.point_sum += toS2Point(point);
        ++sums.points;
    }

    Point get() const
    {
        /// `S2::GetCurvatureMaxError` bounds the error of the area; assume a similar error for the centroid.
        if (std::abs(sums.area) > sums.area_error)
        {
            const S2Point area_centroid = sums.area < 0 ? sums.area_centroid * -1.0 : sums.area_centroid;
            return normalize(area_centroid, sums.area_error + static_cast<double>(sums.rings + 4) * DBL_EPSILON * sums.area_magnitude);
        }
        if (sums.length > 0)
            return normalize(sums.line_centroid, static_cast<double>(sums.edges + 4) * DBL_EPSILON * sums.length);
        if (sums.points > 0)
        {
            const auto points = static_cast<double>(sums.points);
            return normalize(sums.point_sum, (points + 4) * DBL_EPSILON * points);
        }
        return Point{NaN, NaN};
    }

private:
    static S2Point toS2Point(const Point & point)
    {
        /// `S2LatLng::ToPoint` needs a longitude in [−180, 180]; `std::remainder` reduces it exactly.
        double longitude = boost::geometry::get<0>(point);
        if (std::abs(longitude) > 180)
            longitude = std::remainder(longitude, 360.0);
        return S2LatLng::FromDegrees(boost::geometry::get<1>(point), longitude).ToPoint();
    }

    /// Converts the first `n` points to `vertices`, which is reused to avoid an allocation per geometry.
    template <typename Points>
    void toS2Points(const Points & points, size_t n)
    {
        vertices.clear();
        vertices.reserve(n + 1);
        for (size_t i = 0; i < n; ++i)
            vertices.push_back(toS2Point(points[i]));
    }

    /// Adds the polyline in `vertices`, or its first point if its length is zero.
    void addPolyline()
    {
        const S2PointSpan polyline(vertices);
        const double polyline_length = S2::GetLength(polyline).radians();
        if (polyline_length > 0)
        {
            sums.line_centroid += S2::GetCentroid(polyline);
            sums.length += polyline_length;
            sums.edges += vertices.size() - 1;
        }
        else
        {
            sums.point_sum += vertices.front();
            ++sums.points;
        }
    }

    /// The direction of `sum`, or NaN if its length is within the rounding error `max_error`. This happens when the
    /// parts of a geometry cancel out, for example two antipodal points.
    static Point normalize(const S2Point & sum, double max_error)
    {
        if (!(sum.Norm() > max_error))
            return Point{NaN, NaN};
        const S2LatLng lat_lng(sum.Normalize());
        return Point{lat_lng.lng().degrees(), lat_lng.lat().degrees()};
    }

    struct Sums
    {
        S2Point area_centroid;
        double area = 0;
        double area_magnitude = 0;
        double area_error = 0;
        size_t rings = 0;

        S2Point line_centroid;
        double length = 0;
        size_t edges = 0;

        S2Point point_sum;
        size_t points = 0;
    };

    Sums sums;
    std::vector<S2Point> vertices; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
};

#endif

template <typename Accumulator, typename Geometry>
void addGeometry(Accumulator & accumulator, const Geometry & geometry)
{
    using Point = typename Accumulator::Point;
    if constexpr (std::is_same_v<Geometry, Ring<Point>>)
    {
        accumulator.addRing(geometry, /* is_hole */ false);
    }
    else if constexpr (std::is_same_v<Geometry, Polygon<Point>>)
    {
        accumulator.addRing(geometry.outer(), /* is_hole */ false);
        for (const auto & inner : geometry.inners())
            accumulator.addRing(inner, /* is_hole */ true);
    }
    else if constexpr (std::is_same_v<Geometry, MultiPolygon<Point>>)
    {
        for (const auto & polygon : geometry)
            addGeometry(accumulator, polygon);
    }
    else if constexpr (std::is_same_v<Geometry, LineString<Point>>)
    {
        accumulator.addLineString(geometry);
    }
    else if constexpr (std::is_same_v<Geometry, MultiLineString<Point>>)
    {
        for (const auto & linestring : geometry)
            accumulator.addLineString(linestring);
    }
    else
    {
        static_assert(std::is_same_v<Geometry, MultiPoint<Point>>);
        for (const auto & point : geometry)
            accumulator.addPoint(point);
    }
}

template <typename Traits>
class FunctionCentroid : public IFunction
{
public:
    using Point = typename Traits::PointType;
    using Accumulator = typename Traits::Accumulator;
    static constexpr auto name = Traits::name;

    explicit FunctionCentroid() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCentroid>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->getName() != "Geometry" && !getGeometryColumnTypeFromDataType(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} should be a geometry type, got {}",
                getName(),
                arguments[0]->getName());

        return DataTypeFactory::instance().get("Point");
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return DataTypeFactory::instance().get("Point");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        PointSerializer<Point> serializer;
        Accumulator accumulator;

        const auto * column_variant = typeid_cast<const ColumnVariant *>(arguments.front().column.get());
        if (column_variant)
        {
            Field field;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                column_variant->get(i, field);
                auto global_discr = column_variant->globalDiscriminatorAt(i);
                auto type = magic_enum::enum_cast<GeometryColumnType>(global_discr);
                if (!type)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of geometry {}", static_cast<Int32>(global_discr));
                processField(field, *type, serializer, accumulator);
            }
        }
        else
        {
            callOnGeometryDataType<Point>(
                arguments[0].type,
                [&](const auto & type)
                {
                    using TypeConverter = std::decay_t<decltype(type)>;
                    using Converter = typename TypeConverter::Type;

                    auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());
                    for (size_t i = 0; i < input_rows_count; ++i)
                        serializer.add(computeCentroid(geometries[i], accumulator));
                });
        }

        return serializer.finalize();
    }

private:
    template <typename Geometry>
    static Point computeCentroid(const Geometry & geometry, Accumulator & accumulator)
    {
        if constexpr (std::is_same_v<Point, SphericalPoint>)
        {
            checkSphericalLatitudes(geometry, name);
            checkSphericalEdges(geometry, name);
        }

        if constexpr (std::is_same_v<Geometry, SphericalPoint>)
        {
            /// Reduce the longitude to [−180, 180] as for the other geometries, which pass through `S2LatLng`.
            return SphericalPoint(std::remainder(boost::geometry::get<0>(geometry), 360.0), boost::geometry::get<1>(geometry));
        }
        else if constexpr (std::is_same_v<Geometry, Point>)
        {
            return geometry;
        }
        else
        {
            accumulator.reset();
            addGeometry(accumulator, geometry);
            return accumulator.get();
        }
    }

    static void processField(const Field & field, GeometryColumnType type, PointSerializer<Point> & serializer, Accumulator & accumulator)
    {
        auto add = [&](const auto & geometry)
        {
            checkPointsAreFinite(geometry);
            serializer.add(computeCentroid(geometry, accumulator));
        };

        switch (type)
        {
            case GeometryColumnType::Point:
                add(getPointFromField<Point>(field));
                break;
            case GeometryColumnType::Linestring:
                add(getLineStringFromField<Point>(field));
                break;
            case GeometryColumnType::Ring:
                add(getRingFromField<Point>(field));
                break;
            case GeometryColumnType::Polygon:
                add(getPolygonFromField<Point>(field));
                break;
            case GeometryColumnType::MultiLinestring:
                add(getMultiLineStringFromField<Point>(field));
                break;
            case GeometryColumnType::MultiPolygon:
                add(getMultiPolygonFromField<Point>(field));
                break;
            case GeometryColumnType::MultiPoint:
                add(getMultiPointFromField<Point>(field));
                break;
            case GeometryColumnType::Null:
                serializer.add(Point{NaN, NaN});
                break;
        }
    }
};

struct CartesianTraits
{
    using PointType = CartesianPoint;
    using Accumulator = CartesianCentroid;
    static constexpr auto name = "centroidCartesian";
};

#if USE_S2_GEOMETRY
struct SphericalTraits
{
    using PointType = SphericalPoint;
    using Accumulator = SphericalCentroid;
    static constexpr auto name = "centroidSpherical";
};
#endif

}

REGISTER_FUNCTION(Centroid)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the centroid (center of mass) of a geometry in Cartesian coordinates.
Takes any geometry type and returns a Point.

As in JTS and GEOS, polygons are weighted by area, linestrings by length, and points equally.
Holes are subtracted whatever the winding order.
If the polygons have zero area (for example, each ring lies on a line), the result is the centroid of their boundaries;
if those have zero length too, the mean of the vertices.
For an empty geometry, the result is `(nan,nan)`.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "centroidCartesian(geometry)";
    FunctionDocumentation::Arguments arguments_cartesian
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point", "MultiPoint"}}};
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the centroid of the geometry as a Point.", {"Point"}};
    FunctionDocumentation::Examples examples_cartesian
        = {{"Centroid of a square",
            R"(
SELECT centroidCartesian([[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]])
        )",
            R"(
(2,2)
        )"},
           {"Centroid of a ring of zero area is the centroid of its boundary",
            R"(
SELECT centroidCartesian([(0., 0.), (5., 5.), (10., 10.), (0., 0.)])
        )",
            R"(
(5,5)
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {26, 11};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian
        = {description_cartesian,
           syntax_cartesian,
           arguments_cartesian,
           {},
           returned_value_cartesian,
           examples_cartesian,
           introduced_in_cartesian,
           category_cartesian};

    factory.registerFunction<FunctionCentroid<CartesianTraits>>(function_documentation_cartesian);

#if USE_S2_GEOMETRY
    FunctionDocumentation::Description description_spherical = R"(
Calculates the centroid (center of mass) of a geometry on a sphere, with coordinates `(longitude, latitude)` in degrees.
Takes any geometry type and returns a Point. Uses the true centroid (mass centroid)
which correctly accounts for the curvature of the sphere. The longitude of the result is in [−180, 180].

The interior of a ring is the smaller of the two regions it bounds, so the winding order does not matter.
If the two regions are equal (for example, the ring is the equator), the interior is the one on the right of the ring,
as in `boundingBoxSpherical`.
As in `centroidCartesian`, polygons are weighted by area, linestrings by length, and points equally.
If the polygons have zero area (for example, each ring lies on a great circle), the result is the centroid of their
boundaries; if those have zero length too, the mean of the vertices.
For an empty geometry, or when the centroid is undefined (for example, two antipodal points), the result is `(nan,nan)`.
A latitude outside [−90, 90] throws an exception, and so does an edge of a ring or a linestring between two antipodal
points, because its path is undefined.

The rounding error of the area is up to about 2.5·10⁻¹⁵ steradians per vertex, or about 0.1 m² per vertex on the Earth.
A polygon with a smaller area is treated as having zero area, so the result is the centroid of its boundary, which can
differ noticeably from the centroid of its area. For polygons up to about 10 times larger than this error, the error of
the centroid is up to about 1% of the polygon size; for larger polygons it is negligible.
    )";
    FunctionDocumentation::Syntax syntax_spherical = "centroidSpherical(geometry)";
    FunctionDocumentation::Arguments arguments_spherical
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point", "MultiPoint"}}};
    FunctionDocumentation::ReturnedValue returned_value_spherical
        = {"Returns the centroid of the geometry as a Point in spherical coordinates.", {"Point"}};
    FunctionDocumentation::Examples examples_spherical
        = {{"Centroid of a polygon in spherical coordinates",
            R"(
SELECT centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]])
        )",
            R"(
(4.352733722119422,50.846349225815466)
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {26, 11};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical
        = {description_spherical,
           syntax_spherical,
           arguments_spherical,
           {},
           returned_value_spherical,
           examples_spherical,
           introduced_in_spherical,
           category_spherical};

    factory.registerFunction<FunctionCentroid<SphericalTraits>>(function_documentation_spherical);
#endif
}

}
