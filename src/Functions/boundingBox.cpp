#include <Functions/FunctionFactory.h>
#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <Columns/ColumnVariant.h>

#include <Common/VectorWithMemoryTracking.h>

#include <base/EnumReflection.h>

#include <boost/geometry/algorithms/detail/envelope/range_of_boxes.hpp>
#include <boost/geometry/algorithms/envelope.hpp>
#include <boost/geometry/geometries/box.hpp>

#include <algorithm>
#include <cmath>
#include <numbers>
#include <span>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

using SphericalBox = boost::geometry::model::box<SphericalPoint>;

constexpr Float64 radians_per_degree = std::numbers::pi / 180;

/// `longitude` reduced exactly to (−180, 180], as `boost::geometry` normalizes it.
Float64 normalizeLongitude(Float64 longitude)
{
    const Float64 reduced = std::remainder(longitude, 360.0);
    return reduced == -180 ? 180 : reduced;
}

/// The change of longitude along the shorter way from `from` to `to`, in [−180, 180]. It is ±180 if the points are on
/// opposite meridians, so that the edge between them passes through a pole: the same test as in `areAntipodal`.
Float64 longitudeStep(const SphericalPoint & from, const SphericalPoint & to)
{
    return std::remainder(
        normalizeLongitude(boost::geometry::get<0>(to)) - normalizeLongitude(boost::geometry::get<0>(from)), 360.0);
}

bool isPole(const SphericalPoint & point)
{
    return std::abs(boost::geometry::get<1>(point)) == 90;
}

SphericalBox makeBox(Float64 min_longitude, Float64 min_latitude, Float64 max_longitude, Float64 max_latitude)
{
    return SphericalBox(SphericalPoint(min_longitude, min_latitude), SphericalPoint(max_longitude, max_latitude));
}

/// Adds the box of the longitudes east from `west` to `east`, both in (−180, 180], and of the latitudes from `min_latitude`
/// to `max_latitude`. If the longitudes cross the antimeridian, these are two boxes, so that the ends of the longitudes are
/// exact, and boxes that meet at a longitude leave no gap between them.
void addEastwardBox(
    VectorWithMemoryTracking<SphericalBox> & boxes, Float64 west, Float64 east, Float64 min_latitude, Float64 max_latitude)
{
    if (west <= east)
    {
        boxes.push_back(makeBox(west, min_latitude, east, max_latitude));
        return;
    }
    boxes.push_back(makeBox(west, min_latitude, 180, max_latitude));
    boxes.push_back(makeBox(-180, min_latitude, east, max_latitude));
}

/// The lowest and the highest latitude on the great circle arc from a point at `from_latitude` to a point at `to_latitude`,
/// where `step` is the change of longitude along the arc, |`step`| < 180, and neither point is a pole.
std::pair<Float64, Float64> arcLatitudes(Float64 step, Float64 from_latitude, Float64 to_latitude)
{
    Float64 min_latitude = std::min(from_latitude, to_latitude);
    Float64 max_latitude = std::max(from_latitude, to_latitude);

    const Float64 sin_from = std::sin(from_latitude * radians_per_degree);
    const Float64 cos_from = std::cos(from_latitude * radians_per_degree);
    const Float64 sin_to = std::sin(to_latitude * radians_per_degree);
    const Float64 cos_to = std::cos(to_latitude * radians_per_degree);
    const Float64 sin_half_step = std::sin(step * radians_per_degree / 2);
    const Float64 versine = 2 * sin_half_step * sin_half_step;
    const Float64 sin_difference = std::sin((to_latitude - from_latitude) * radians_per_degree);

    /// The rate at which the latitude grows along the arc at `from`, and along the reversed arc at `to`, up to positive
    /// factors. They are written with the difference of the latitudes, so that they are accurate for short arcs.
    const Float64 rise_from = sin_difference + sin_from * cos_to * versine;
    const Float64 rise_to = -sin_difference + sin_to * cos_from * versine;
    const bool highest = rise_from > 0 && rise_to > 0;
    const bool lowest = rise_from < 0 && rise_to < 0;
    if (highest || lowest)
    {
        /// The arc passes through the point of its great circle farthest from the equator. If `from` has longitude 0, the
        /// normal of the great circle, the cross product of `from` and `to`, is (x, −`rise_from`, z) below, and the
        /// latitude of this point is the angle between the normal and the axis of the sphere. It is accurate also when
        /// the arc passes near a pole, where tests of the azimuths of the arc are not.
        const Float64 sin_step = std::sin(step * radians_per_degree);
        const Float64 x = -sin_from * cos_to * sin_step;
        const Float64 z = cos_from * cos_to * sin_step;
        const Float64 extreme = std::atan2(std::hypot(x, rise_from), std::abs(z)) / radians_per_degree;
        if (highest)
            max_latitude = std::max(max_latitude, extreme);
        else
            min_latitude = std::min(min_latitude, -extreme);
    }
    return {min_latitude, max_latitude};
}

/// The area between the great circle arc from a point at `from_latitude` to a point at `to_latitude` and the equator,
/// where `step` is the change of longitude along the arc, |`step`| < 180. This is the integral of sin(latitude)
/// d(longitude) along the arc, which the formula gives exactly.
Float64 edgeIntegral(Float64 step, Float64 from_latitude, Float64 to_latitude)
{
    const Float64 t1 = std::tan(from_latitude * radians_per_degree / 2);
    const Float64 t2 = std::tan(to_latitude * radians_per_degree / 2);
    return 2 * std::atan(std::tan(step * radians_per_degree / 2) * (t1 + t2) / (1 + t1 * t2));
}

/// The region on the right of a ring, see `walkRing`.
struct RingWalk
{
    /// In steradians.
    Float64 area = 0;
    /// Whether the region contains the north pole or the south pole. A pole on the ring is not in the region, unless the
    /// ring only goes to the pole and back along the same meridian.
    bool north = false;
    bool south = false;
    /// Whether the ring goes around the north pole or the south pole on a tiny circle, see `walkRing`. Then the pole is
    /// in neither of the two regions the ring bounds.
    bool north_on_ring = false;
    bool south_on_ring = false;
    /// The longitudes that the region covers near the poles on the ring.
    VectorWithMemoryTracking<SphericalBox> wedges;
    /// The latitude of a vertex that is not a pole, if there is one.
    Float64 latitude = 0;
};

/// Walks along the `n` vertices of `ring`, backwards if `reversed`, and finds the region on the right of the ring.
///
/// The integral of sin(latitude) d(longitude) along the ring is the area of this region minus 2π for each pole in it.
/// The total change of longitude along the ring tells which poles these are: the south pole if the ring goes east around
/// the axis of the sphere, the north pole if it goes west, and either both or none, as the sign of the integral tells, if
/// it does not go around it. This does not depend on the vertex the ring starts at, unlike the winding test of
/// `boost::geometry::envelope`, which can miss a pole inside a ring.
///
/// A pole on the ring, at a vertex or on an edge between opposite meridians, is moved out of the region: the ring goes
/// around it along a tiny circle, east around the north pole and west around the south pole, so that the pole is on its
/// left. Near the pole, the region covers the longitudes of this detour.
RingWalk walkRing(const Ring<SphericalPoint> & ring, size_t n, bool reversed)
{
    auto vertex = [&](size_t i) -> const SphericalPoint &
    {
        i %= n;
        return ring[reversed && i != 0 ? n - i : i];
    };

    RingWalk walk;
    size_t start = 0;
    while (start < n && isPole(vertex(start)))
        ++start;
    if (start == n)
        return walk;
    walk.latitude = boost::geometry::get<1>(vertex(start));

    /// In degrees.
    Float64 turn = 0;
    Float64 integral = 0;
    const SphericalPoint * from = &vertex(start);
    /// 1 or −1 if the ring passes through the north or the south pole after `from`.
    int pole = 0;
    for (size_t i = start + 1; i <= start + n; ++i)
    {
        const SphericalPoint & to = vertex(i);
        if (isPole(to))
        {
            pole = boost::geometry::get<1>(to) > 0 ? 1 : -1;
            continue;
        }

        const Float64 from_latitude = boost::geometry::get<1>(*from);
        const Float64 to_latitude = boost::geometry::get<1>(to);
        const Float64 step = longitudeStep(*from, to);
        if (pole == 0 && std::abs(step) == 180)
            pole = from_latitude + to_latitude > 0 ? 1 : -1;

        if (pole == 0)
        {
            turn += step;
            integral += edgeIntegral(step, from_latitude, to_latitude);
        }
        else
        {
            Float64 detour = step;
            if (pole > 0 && detour < 0)
                detour += 360;
            else if (pole < 0 && detour > 0)
                detour -= 360;
            turn += detour;
            integral += pole * detour * radians_per_degree;
            /// A zero detour is a spike to the pole and back, which covers no longitudes.
            if (detour != 0)
            {
                const Float64 from_longitude = normalizeLongitude(boost::geometry::get<0>(*from));
                const Float64 to_longitude = normalizeLongitude(boost::geometry::get<0>(to));
                if (pole > 0)
                {
                    walk.north_on_ring = true;
                    addEastwardBox(walk.wedges, from_longitude, to_longitude, from_latitude, 90);
                }
                else
                {
                    walk.south_on_ring = true;
                    addEastwardBox(walk.wedges, to_longitude, from_longitude, -90, from_latitude);
                }
            }
            pole = 0;
        }
        from = &to;
    }

    walk.area = integral;
    const Float64 winding = std::round(turn / 360);
    if (winding > 0)
    {
        walk.south = true;
        walk.area += 2 * std::numbers::pi;
    }
    else if (winding < 0)
    {
        walk.north = true;
        walk.area += 2 * std::numbers::pi;
    }
    else if (integral < 0)
    {
        walk.north = true;
        walk.south = true;
        walk.area += 4 * std::numbers::pi;
    }
    return walk;
}

/// `walkRing` in the direction that puts the interior of `ring` on the right, or its exterior if `exterior`. The
/// interior is the smaller of the two regions the ring bounds. If they are equal (`isHemisphere`), it is the region on
/// the right of the ring, as in `centroidSpherical`.
RingWalk walkRegion(const Ring<SphericalPoint> & ring, size_t n, bool exterior)
{
    RingWalk forward = walkRing(ring, n, /* reversed */ false);
    const bool interior_on_left = !isHemisphere(forward.area, ringEdgeCount(ring, n)) && forward.area > 2 * std::numbers::pi;
    if (interior_on_left == exterior)
        return forward;

    /// The region on the left is the rest of the sphere. Its poles are not taken from the backward walk: if the ring
    /// bounds no area, rounding errors can put both poles or none on both sides of it.
    RingWalk backward = walkRing(ring, n, /* reversed */ true);
    backward.area = 4 * std::numbers::pi - forward.area;
    backward.north = !forward.north && !forward.north_on_ring;
    backward.south = !forward.south && !forward.south_on_ring;
    return backward;
}

/// Collects boxes that cover the parts of a geometry on the sphere, and merges them into the smallest box that covers
/// all of them. Unlike `boost::geometry::expand`, which merges two boxes at a time, the result does not depend on the
/// order of the parts.
class SphericalBoxes
{
public:
    template <typename Geometry>
    void add(const Geometry & geometry)
    {
        using Tag = typename boost::geometry::tag<Geometry>::type;
        if constexpr (std::is_same_v<Tag, boost::geometry::point_tag>)
            addPoint(geometry);
        else if constexpr (std::is_same_v<Tag, boost::geometry::linestring_tag>)
            addPath(geometry, geometry.size(), /* closed */ false);
        else if constexpr (std::is_same_v<Tag, boost::geometry::ring_tag>)
            addPolygon(geometry, {});
        else if constexpr (std::is_same_v<Tag, boost::geometry::polygon_tag>)
            addPolygon(geometry.outer(), geometry.inners());
        else
            for (const auto & part : geometry)
                add(part);
    }

    /// Returns false if there are no boxes, because the geometry is empty.
    bool merge(SphericalBox & result) const
    {
        if (boxes.empty())
            return false;
        boost::geometry::detail::envelope::envelope_range_of_boxes::apply(boxes, result);
        return true;
    }

private:
    VectorWithMemoryTracking<SphericalBox> boxes;

    /// The longitude of a pole does not matter: `envelope_range_of_boxes` ignores the longitudes of a box at a pole.
    void addPoint(const SphericalPoint & point)
    {
        const Float64 longitude = normalizeLongitude(boost::geometry::get<0>(point));
        const Float64 latitude = boost::geometry::get<1>(point);
        boxes.push_back(makeBox(longitude, latitude, longitude, latitude));
    }

    /// The shorter great circle arc from `from` to `to`, which are not antipodal.
    void addEdge(const SphericalPoint & from, const SphericalPoint & to)
    {
        const Float64 from_longitude = normalizeLongitude(boost::geometry::get<0>(from));
        const Float64 to_longitude = normalizeLongitude(boost::geometry::get<0>(to));
        const Float64 from_latitude = boost::geometry::get<1>(from);
        const Float64 to_latitude = boost::geometry::get<1>(to);
        const Float64 min_latitude = std::min(from_latitude, to_latitude);
        const Float64 max_latitude = std::max(from_latitude, to_latitude);

        if (isPole(from) || isPole(to))
        {
            /// The arc goes along the meridian of the other end.
            const Float64 longitude = isPole(from) ? to_longitude : from_longitude;
            boxes.push_back(makeBox(longitude, min_latitude, longitude, max_latitude));
            return;
        }

        const Float64 step = longitudeStep(from, to);
        if (std::abs(step) == 180)
        {
            /// The arc goes along the meridian of `from` to a pole and along the meridian of `to` from it, and covers no
            /// longitudes between these meridians.
            const Float64 pole = from_latitude + to_latitude > 0 ? 90 : -90;
            boxes.push_back(makeBox(from_longitude, std::min(from_latitude, pole), from_longitude, std::max(from_latitude, pole)));
            boxes.push_back(makeBox(to_longitude, std::min(to_latitude, pole), to_longitude, std::max(to_latitude, pole)));
            return;
        }

        /// The longitude changes monotonically along the arc, by `step`.
        const auto [arc_min_latitude, arc_max_latitude] = arcLatitudes(step, from_latitude, to_latitude);
        if (step >= 0)
            addEastwardBox(boxes, from_longitude, to_longitude, arc_min_latitude, arc_max_latitude);
        else
            addEastwardBox(boxes, to_longitude, from_longitude, arc_min_latitude, arc_max_latitude);
    }

    /// The first `n` points of `points`, joined by edges, and the last one is joined to the first one if `closed`.
    template <typename Points>
    void addPath(const Points & points, size_t n, bool closed)
    {
        if (n == 1)
            addPoint(points[0]);
        for (size_t i = 0; i + 1 < n; ++i)
            addEdge(points[i], points[i + 1]);
        if (closed && n > 2)
            addEdge(points[n - 1], points[0]);
    }

    /// The interior of each ring is the smaller of the two regions it bounds, see `walkRegion`. The box of the polygon is
    /// the box of its rings, expanded to the longitudes it covers near a pole on a ring, and to all longitudes and the
    /// latitude of a pole inside it.
    void addPolygon(const Ring<SphericalPoint> & outer, std::span<const Ring<SphericalPoint>> holes)
    {
        const size_t n = ringVertexCount(outer);
        addPath(outer, n, /* closed */ true);
        for (const auto & hole : holes)
            addPath(hole, ringVertexCount(hole), /* closed */ true);
        /// A ring with less than three vertices bounds no region.
        if (n < 3)
            return;

        const RingWalk walk = walkRegion(outer, n, /* exterior */ false);
        boxes.insert(boxes.end(), walk.wedges.begin(), walk.wedges.end());
        bool north = walk.north;
        bool south = walk.south;

        for (const auto & hole : holes)
        {
            const size_t hole_n = ringVertexCount(hole);
            if (hole_n < 3)
                continue;
            const RingWalk outside = walkRegion(hole, hole_n, /* exterior */ true);
            boxes.insert(boxes.end(), outside.wedges.begin(), outside.wedges.end());
            north = north && outside.north;
            south = south && outside.south;
        }

        if (north)
            boxes.push_back(makeBox(-180, walk.latitude, 180, 90));
        if (south)
            boxes.push_back(makeBox(-180, -90, 180, walk.latitude));
    }
};

template <typename Traits>
class FunctionBoundingBox : public IFunction
{
public:
    using Point = typename Traits::PointType;
    static constexpr auto name = Traits::name;

    explicit FunctionBoundingBox() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBoundingBox>(); }

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

        return DataTypeFactory::instance().get("Ring");
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return DataTypeFactory::instance().get("Ring");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Geometry functions work with the Geometry type directly which is a Variant with custom name,
    /// and not with individual variant alternatives. So, don't use default implementation.
    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        RingSerializer<Point> serializer;

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
                processField(field, *type, serializer);
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
                    {
                        Ring<Point> ring = computeEnvelope(geometries[i]);
                        serializer.add(ring);
                    }
                });
        }

        return serializer.finalize();
    }

private:
    /// Convert a boost::geometry::model::box to a Ring (5-point closed rectangle).
    /// Note: For spherical coordinates crossing the date line, Boost may return
    /// longitude values > 180 (e.g., 181 represents -179). This is intentional
    /// and mathematically correct - normalizing to [-180, 180] would produce a
    /// ring spanning ~358° instead of the actual ~2°.
    static Ring<Point> boxToRing(const boost::geometry::model::box<Point> & box)
    {
        auto min_x = box.min_corner().template get<0>();
        auto min_y = box.min_corner().template get<1>();
        auto max_x = box.max_corner().template get<0>();
        auto max_y = box.max_corner().template get<1>();

        Ring<Point> ring;
        ring.reserve(5);
        ring.emplace_back(min_x, min_y);
        ring.emplace_back(max_x, min_y);
        ring.emplace_back(max_x, max_y);
        ring.emplace_back(min_x, max_y);
        ring.emplace_back(min_x, min_y);
        return ring;
    }

    template <typename Geom>
    static Ring<Point> computeEnvelope(const Geom & geom)
    {
        boost::geometry::model::box<Point> box{};
        if constexpr (std::is_same_v<Point, SphericalPoint>)
        {
            checkSphericalLatitudes(geom, name);
            checkSphericalEdges(geom, name);
            SphericalBoxes boxes;
            boxes.add(geom);
            if (!boxes.merge(box))
                return {};
        }
        else
        {
            boost::geometry::envelope(geom, box);
            /// An empty geometry has no bounding box: `boost::geometry::envelope` leaves the box inverted.
            if (box.min_corner().template get<0>() > box.max_corner().template get<0>())
                return {};
        }
        return boxToRing(box);
    }

    static void processField(const Field & field, GeometryColumnType type, RingSerializer<Point> & serializer)
    {
        auto add = [&](auto geometry)
        {
            checkPointsAreFinite(geometry);
            serializer.add(computeEnvelope(geometry));
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
                serializer.add(Ring<Point>{});
                break;
        }
    }
};

struct CartesianTraits
{
    using PointType = CartesianPoint;
    static constexpr auto name = "boundingBoxCartesian";
};

struct SphericalTraits
{
    using PointType = SphericalPoint;
    static constexpr auto name = "boundingBoxSpherical";
};

}

REGISTER_FUNCTION(BoundingBox)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the axis-aligned bounding box of a geometry in Cartesian coordinates.
Returns a Ring (closed rectangle with sides parallel to the coordinate axes).
For an empty geometry, the result is an empty Ring `[]`.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "boundingBoxCartesian(geometry)";
    FunctionDocumentation::Arguments arguments_cartesian
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point", "MultiPoint"}}};
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the bounding box as a Ring (closed rectangle).", {"Ring"}};
    FunctionDocumentation::Examples examples_cartesian
        = {{"Bounding box of a triangle",
            R"(
SELECT boundingBoxCartesian([[(0., 0.), (3., 5.), (6., 0.), (0., 0.)]])
        )",
            R"(
[(0,0),(6,0),(6,5),(0,5),(0,0)]
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

    factory.registerFunction<FunctionBoundingBox<CartesianTraits>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Calculates the bounding box of a geometry in spherical coordinates.
Returns a Ring (closed rectangle). For geometries crossing the date line (±180°),
the returned longitude values may exceed 180° (e.g., 181 represents -179°).
This is the correct mathematical representation; normalizing to [-180, 180] would
produce an incorrect box spanning ~358° instead of ~2°.

The interior of a ring is the smaller of the two regions it bounds, so the winding order does not matter: for example,
the box of a ring around a pole includes the pole. If the two regions are equal (for example, the ring is the equator),
the interior is the one on the right of the ring, as in `centroidSpherical`.
The box is the smallest one that contains the whole geometry, whatever the order of its parts and vertices. For example,
an edge between two points on opposite meridians passes through a pole, so the box includes the pole, but not the
longitudes between the two meridians.
For an empty geometry, the result is an empty Ring `[]`.
A latitude outside [−90, 90] throws an exception, and so does an edge of a ring or a linestring between two antipodal
points, because its path is undefined.
    )";
    FunctionDocumentation::Syntax syntax_spherical = "boundingBoxSpherical(geometry)";
    FunctionDocumentation::Arguments arguments_spherical
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point", "MultiPoint"}}};
    FunctionDocumentation::ReturnedValue returned_value_spherical
        = {"Returns the bounding box as a Ring (closed rectangle) in spherical coordinates.", {"Ring"}};
    FunctionDocumentation::Examples examples_spherical
        = {{"Bounding box of a polygon in spherical coordinates",
            R"(
SELECT boundingBoxSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]])
        )",
            R"(
[(4.338074,50.833264),(4.367945,50.833264),(4.367945,50.858306),(4.338074,50.858306),(4.338074,50.833264)]
        )"},
           {"Bounding box crossing the date line (note longitude > 180°)",
            R"(
SELECT boundingBoxSpherical(CAST([(179., 50.), (-179., 50.)], 'LineString'))
        )",
            R"(
[(179,50),(181,50),(181,50.00429719577797),(179,50.00429719577797),(179,50)]
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

    factory.registerFunction<FunctionBoundingBox<SphericalTraits>>(function_documentation_spherical);
}

}
