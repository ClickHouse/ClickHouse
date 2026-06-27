#include <cmath>
#include <limits>
#include <numbers>

#include "config.h"

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>

#if USE_WAGYU
#include <mapbox/geometry/wagyu/wagyu.hpp>
#endif

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

namespace bg = boost::geometry;

using BPoint = CartesianPoint;
using BBox = bg::model::box<BPoint>;

/// Global discriminator order of the `Geometry` Variant: alternatives are sorted alphabetically by type name.
namespace GeoDisc
{
    constexpr UInt8 LineString = 0;
    constexpr UInt8 MultiLineString = 1;
    constexpr UInt8 MultiPolygon = 2;
    constexpr UInt8 Point = 3;
    constexpr UInt8 Polygon = 4;
    constexpr UInt8 Ring = 5;
}

/// The Web Mercator projection spans 2^32 units on each axis (the standard XYZ tile scheme).
constexpr double mercator_max = 4294967296.0; /// 2^32
/// Latitude bound of the Web Mercator projection (it diverges at the poles).
constexpr double latitude_limit = 85.05112877980659;
#if USE_WAGYU
/// wagyu normalizes collinear edges with Int64 products of coordinate deltas (`slopes_equal`); a delta is at most
/// the width of the clip window, so the product overflows Int64 once the window half-width approaches 2^31 (this
/// includes the corners of the window itself - a 2^31 half-width gives a 2^32 edge whose squared delta is 2^64).
/// The non-clipping path therefore bounds geometry to a 2^30 window: a 2^31-wide edge keeps every product at 2^62
/// (< 2^63), and 2^30 is exactly the pixel span of the whole world at zoom 18 / extent 4096 (2^18 * 4096), so
/// realistic geometry is validated but never clipped; only geometry projected beyond it (extreme zoom/extent) is.
constexpr double max_wagyu_coordinate = 1073741824.0; /// 2^30
#endif

/// Per-row projection from geographic (lon, lat) degrees to tile-local pixel space.
struct Projection
{
    double tile_x_begin = 0;
    double tile_y_begin = 0;
    double scale = 0;

    void operator()(BPoint & p) const
    {
        const double lon = std::clamp(bg::get<0>(p), -180.0, 180.0);
        const double lat = std::clamp(bg::get<1>(p), -latitude_limit, latitude_limit);
        const double mercator_x = mercator_max * (lon + 180.0) / 360.0;
        const double mercator_y
            = mercator_max * (0.5 - std::log(std::tan((lat + 90.0) / 360.0 * std::numbers::pi)) / (2.0 * std::numbers::pi));
        bg::set<0>(p, (mercator_x - tile_x_begin) * scale);
        bg::set<1>(p, (mercator_y - tile_y_begin) * scale);
    }
};

/// Snap every coordinate of a (tile-space) geometry to the integer pixel grid, matching the MVT geometry model and
/// letting `GROUP BY` collapse sub-pixel duplicates into a single cluster.
/// Rounds to the nearest integer (ties to even, via rint, matching PostGIS) and normalises negative zero to positive
/// zero so coordinates clipped to an axis do not surface as "-0".
inline Float64 roundCoordinate(Float64 value)
{
    const Float64 rounded = std::rint(value);
    return rounded == 0.0 ? 0.0 : rounded;
}

template <typename Geometry>
void roundToGrid(Geometry & geometry)
{
    bg::for_each_point(geometry, [](BPoint & p)
    {
        bg::set<0>(p, roundCoordinate(bg::get<0>(p)));
        bg::set<1>(p, roundCoordinate(bg::get<1>(p)));
    });
}

#if USE_WAGYU
/// Conversions between the integer-grid Boost polygons and wagyu's integer geometry, used to clip and validate
/// polygons (wagyu produces MVT-valid output: no self-intersections, correct ring nesting). Coordinates are already
/// whole integers after roundToGrid, so the Float64 <-> Int64 casts are exact.
using WagyuRing = mapbox::geometry::linear_ring<Int64>;

WagyuRing toWagyuRing(const Ring<BPoint> & ring)
{
    WagyuRing out;
    out.reserve(ring.size());
    for (const auto & p : ring)
        out.emplace_back(static_cast<Int64>(bg::get<0>(p)), static_cast<Int64>(bg::get<1>(p)));
    return out;
}

MultiPolygon<BPoint> fromWagyu(const mapbox::geometry::multi_polygon<Int64> & solution)
{
    MultiPolygon<BPoint> result;
    result.reserve(solution.size());
    for (const auto & poly : solution)
    {
        if (poly.empty())
            continue;
        Polygon<BPoint> out;
        for (const auto & pt : poly.front()) /// the first ring is the exterior, the rest are holes
            out.outer().emplace_back(static_cast<Float64>(pt.x), static_cast<Float64>(pt.y));
        for (size_t i = 1; i < poly.size(); ++i)
        {
            out.inners().emplace_back();
            for (const auto & pt : poly[i])
                out.inners().back().emplace_back(static_cast<Float64>(pt.x), static_cast<Float64>(pt.y));
        }
        result.push_back(std::move(out));
    }
    return result;
}

/// Sutherland-Hodgman clip of a ring against the axis-aligned box [lo_x, hi_x] x [lo_y, hi_y]. Unlike
/// bg::intersection it works on any ring, including self-intersecting ("bow-tie") ones, and it bounds the
/// output coordinates to the box so the Int64 arithmetic in wagyu (slopes_equal) cannot overflow for geometry
/// far from the tile. The clipped ring may still be self-intersecting and may include connector edges along the
/// box boundary; wagyu repairs it into valid MVT polygons. Clip-introduced crossing points are rounded back to
/// the integer grid (the input ring is already snapped).
Ring<BPoint> clipRingToBox(const Ring<BPoint> & ring, Float64 lo_x, Float64 lo_y, Float64 hi_x, Float64 hi_y)
{
    Ring<BPoint> poly = ring;
    /// Drop a duplicate closing vertex; the clip treats the vertex list as implicitly closed.
    if (poly.size() >= 2 && bg::get<0>(poly.front()) == bg::get<0>(poly.back()) && bg::get<1>(poly.front()) == bg::get<1>(poly.back()))
        poly.pop_back();

    /// Clip the vertex list against one half-plane: axis 0 = x, axis 1 = y; keep_lower keeps coord >= bound,
    /// otherwise coord <= bound.
    auto clip_edge = [](const Ring<BPoint> & input, int axis, Float64 bound, bool keep_lower)
    {
        Ring<BPoint> output;
        const size_t n = input.size();
        if (n == 0)
            return output;
        auto coord = [axis](const BPoint & p) { return axis == 0 ? bg::get<0>(p) : bg::get<1>(p); };
        auto inside = [&](const BPoint & p) { return keep_lower ? coord(p) >= bound : coord(p) <= bound; };
        auto intersect = [axis, bound](const BPoint & a, const BPoint & b)
        {
            const Float64 ax = bg::get<0>(a);
            const Float64 ay = bg::get<1>(a);
            const Float64 bx = bg::get<0>(b);
            const Float64 by = bg::get<1>(b);
            if (axis == 0)
            {
                const Float64 t = (bound - ax) / (bx - ax);
                return BPoint(bound, ay + t * (by - ay));
            }
            const Float64 t = (bound - ay) / (by - ay);
            return BPoint(ax + t * (bx - ax), bound);
        };
        for (size_t i = 0; i < n; ++i)
        {
            const BPoint & cur = input[i];
            const BPoint & prev = input[(i + n - 1) % n];
            const bool cur_in = inside(cur);
            if (cur_in)
            {
                if (!inside(prev))
                    output.push_back(intersect(prev, cur));
                output.push_back(cur);
            }
            else if (inside(prev))
            {
                output.push_back(intersect(prev, cur));
            }
        }
        return output;
    };

    poly = clip_edge(poly, 0, lo_x, true);
    poly = clip_edge(poly, 0, hi_x, false);
    poly = clip_edge(poly, 1, lo_y, true);
    poly = clip_edge(poly, 1, hi_y, false);

    /// Round the clip-introduced crossing points back to the integer grid (the input ring is already snapped).
    roundToGrid(poly);
    return poly;
}
#endif

/// Accumulates the per-row tile-space geometries into a `Geometry` (Variant) result column.
struct GeometryVariantBuilder
{
    PointSerializer<BPoint> point;
    LineStringSerializer<BPoint> line_string;
    MultiLineStringSerializer<BPoint> multi_line_string;
    PolygonSerializer<BPoint> polygon;
    MultiPolygonSerializer<BPoint> multi_polygon;
    RingSerializer<BPoint> ring;
    ColumnVariant::ColumnDiscriminators::MutablePtr discriminators = ColumnVariant::ColumnDiscriminators::create();

    void addNull() { discriminators->insertValue(ColumnVariant::NULL_DISCRIMINATOR); }

    void addPoint(const BPoint & p)
    {
        point.add(p);
        discriminators->insertValue(GeoDisc::Point);
    }

    void addMultiLineString(const MultiLineString<BPoint> & value)
    {
        multi_line_string.add(value);
        discriminators->insertValue(GeoDisc::MultiLineString);
    }

    void addMultiPolygon(const MultiPolygon<BPoint> & value)
    {
        multi_polygon.add(value);
        discriminators->insertValue(GeoDisc::MultiPolygon);
    }

    ColumnPtr finalize()
    {
        Columns columns;
        columns.push_back(line_string.finalize());        /// 0 LineString
        columns.push_back(multi_line_string.finalize());  /// 1 MultiLineString
        columns.push_back(multi_polygon.finalize());      /// 2 MultiPolygon
        columns.push_back(point.finalize());              /// 3 Point
        columns.push_back(polygon.finalize());            /// 4 Polygon
        columns.push_back(ring.finalize());               /// 5 Ring
        return ColumnVariant::create(std::move(discriminators), columns);
    }
};

/// MVTEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]]) -> Geometry
///
/// Projects a geometry (in geographic lon/lat) into the tile-local pixel space of the slippy-map tile identified by
/// zoom/tile_x/tile_y, snaps it to the integer pixel grid, optionally clips it to the tile expanded by `buffer` pixels,
/// and returns the tile-space geometry as a `Geometry` (NULL when the geometry falls entirely outside the clip window). The
/// result feeds the aggregate function `MVTEncode`. Analogue of PostGIS `ST_AsMVTGeom` (registered alias `ST_AsMVTGeom`).
class FunctionMVTEncodeGeom final : public IFunction
{
public:
    static constexpr auto name = "MVTEncodeGeom";

    static constexpr UInt64 default_extent = 4096;
    static constexpr UInt64 default_buffer = 1;
    static constexpr Int64 max_extent_or_buffer = std::numeric_limits<Int32>::max();

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMVTEncodeGeom>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    /// Disabled: the result `Geometry` (a Variant) cannot be wrapped in Nullable, so the default null adaptor would
    /// strip nullability and evaluate null rows with the default value, leaking a spurious feature. Nullability is
    /// handled explicitly in executeImpl (any null argument yields a NULL geometry for that row).
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 4 || arguments.size() > 7)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} requires 4 to 7 arguments (geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]]), got {}",
                getName(),
                arguments.size());

        const auto geometry_type = removeNullable(arguments[0].type);
        if (geometry_type->getName() != "Geometry" && !getGeometryColumnTypeFromDataType(geometry_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} must be a geometry (Point, LineString, MultiLineString, Ring, Polygon, "
                "MultiPolygon or Geometry), got {}",
                getName(),
                arguments[0].type->getName());

        /// zoom, tile_x, tile_y, extent, buffer and clip must be unsigned integers (matching MVTBoundingBox); accepting
        /// arbitrary numeric types would silently truncate fractional values such as a zoom of 1.9 to 1.
        for (size_t i = 1; i < arguments.size(); ++i)
            if (!isNativeUInt(removeNullable(arguments[i].type)))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument #{} of function {} must be an unsigned integer, got {}",
                    i + 1,
                    getName(),
                    arguments[i].type->getName());

        return DataTypeFactory::instance().get("Geometry");
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto full = arguments;
        for (auto & a : full)
            a.column = a.column->convertToFullColumnIfConst();

        /// Null propagation is handled here (the default adaptor is disabled): a row is NULL if any argument is NULL,
        /// and such rows emit a NULL geometry. Read from the unwrapped (nested) columns for the remaining rows.
        NullMap null_rows(input_rows_count, 0);
        bool has_nulls = false;
        for (auto & a : full)
        {
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(a.column.get()))
            {
                has_nulls = true;
                const auto & map = nullable->getNullMapData();
                for (size_t row = 0; row < input_rows_count; ++row)
                    null_rows[row] |= map[row];
                a.column = nullable->getNestedColumnPtr();
            }
        }

        const IColumn & col_zoom = *full[1].column;
        const IColumn & col_tile_x = *full[2].column;
        const IColumn & col_tile_y = *full[3].column;
        const bool has_extent = full.size() >= 5;
        const bool has_buffer = full.size() >= 6;
        const bool has_clip = full.size() >= 7;
        const IColumn * col_extent = has_extent ? full[4].column.get() : nullptr;
        const IColumn * col_buffer = has_buffer ? full[5].column.get() : nullptr;
        const IColumn * col_clip = has_clip ? full[6].column.get() : nullptr;

        GeometryVariantBuilder builder;

        /// Resolve, per row, the projection and clip window, then dispatch on the geometry kind.
        auto row_context = [&](size_t row, Projection & projection, BBox & box, bool & clip)
        {
            const UInt64 zoom = col_zoom.getUInt(row);
            if (zoom > 32)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The zoom argument ({}) of function {} must be in [0, 32]", zoom, getName());

            const UInt64 extent = has_extent ? col_extent->getUInt(row) : default_extent;
            if (extent == 0 || extent > static_cast<UInt64>(max_extent_or_buffer))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The extent argument of function {} must be in [1, {}]", getName(), max_extent_or_buffer);

            const UInt64 buffer = has_buffer ? col_buffer->getUInt(row) : default_buffer;
            if (buffer > static_cast<UInt64>(max_extent_or_buffer))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The buffer argument of function {} must be in [0, {}]", getName(), max_extent_or_buffer);

            clip = has_clip ? (col_clip->getUInt(row) != 0) : true;

            /// When clipping, coordinates lie in [-buffer, extent + buffer], so a segment across the buffered tile can
            /// have a delta of extent + 2 * buffer; require it to fit Int32 so the clipped result is always encodable by
            /// MVTEncode. With clipping disabled the buffer is unused, and any out-of-range coordinate or delta is
            /// rejected by MVTEncode instead.
            if (clip && extent + 2 * buffer > static_cast<UInt64>(max_extent_or_buffer))
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "extent + 2 * buffer ({}) of function {} must not exceed {} so the clipped geometry fits the MVT command stream",
                    extent + 2 * buffer, getName(), max_extent_or_buffer);

            const UInt64 tile_x = col_tile_x.getUInt(row);
            const UInt64 tile_y = col_tile_y.getUInt(row);
            const UInt64 num_tiles = UInt64{1} << zoom;
            if (tile_x >= num_tiles || tile_y >= num_tiles)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The tile indices (tile_x = {}, tile_y = {}) of function {} must be less than 2^zoom = {}",
                    tile_x, tile_y, getName(), num_tiles);

            const double tile_size = std::exp2(static_cast<double>(32 - zoom));
            projection = Projection{static_cast<double>(tile_x) * tile_size, static_cast<double>(tile_y) * tile_size, static_cast<double>(extent) / tile_size};
            const double lo = -static_cast<double>(buffer);
            const double hi = static_cast<double>(extent) + static_cast<double>(buffer);
            box = BBox(BPoint(lo, lo), BPoint(hi, hi));
        };

        /// Process one geometry of a concrete Boost type into the result builder.
        auto process_point = [&](BPoint p, const Projection & projection, const BBox & box, bool clip)
        {
            projection(p);
            /// Snap to the integer pixel grid before clipping, so a coordinate a fraction of a pixel outside the
            /// tile rounds onto the boundary pixel and is kept rather than dropped.
            bg::set<0>(p, roundCoordinate(bg::get<0>(p)));
            bg::set<1>(p, roundCoordinate(bg::get<1>(p)));
            if (clip && !bg::covered_by(p, box))
            {
                builder.addNull();
                return;
            }
            builder.addPoint(p);
        };

        auto process_lines = [&](MultiLineString<BPoint> lines, const Projection & projection, const BBox & box, bool clip)
        {
            bg::for_each_point(lines, projection);
            /// Snap to the integer pixel grid before clipping (see process_point), then round again after clipping
            /// because intersecting with the box can introduce fractional crossing points on its edges.
            roundToGrid(lines);
            MultiLineString<BPoint> result;
            if (clip)
                bg::intersection(lines, box, result);
            else
                result = std::move(lines);
            /// A single empty input is wrapped into a Multi container holding one empty element, so the
            /// container is non-empty despite having no vertices; check the vertex count to map it to NULL.
            if (bg::num_points(result) == 0)
            {
                builder.addNull();
                return;
            }
            roundToGrid(result);
            builder.addMultiLineString(result);
        };

        auto process_polygons = [&](MultiPolygon<BPoint> polygons, const Projection & projection, [[maybe_unused]] const BBox & box, bool clip)
        {
            bg::for_each_point(polygons, projection);
            /// Snap to the integer pixel grid before clipping (clip-vs-snap ordering, matching PostGIS).
            roundToGrid(polygons);

            /// A single empty input is wrapped into a Multi container holding one empty element, so the
            /// container is non-empty despite having no vertices; check the vertex count to map it to NULL.
            if (bg::num_points(polygons) == 0)
            {
                builder.addNull();
                return;
            }

#if USE_WAGYU
            /// Validate (and optionally clip) with wagyu: unlike bg::intersection it repairs self-intersections
            /// and nested rings into MVT-valid polygons. Both the clipping and non-clipping paths run through
            /// wagyu (as PostGIS does); the only difference is the clip window - the tile-plus-buffer box when
            /// clipping, otherwise the full 2^31 coordinate range so realistic geometry is validated but not
            /// clipped. wagyu normalizes collinear edges with Int64 products of coordinate deltas that overflow
            /// past 2^31, so each ring is first Sutherland-Hodgman pre-clipped to the window (bounding the
            /// coordinates); the even-odd fill rule then resolves self-intersections independently of the input
            /// ring winding.
            /// Default to the full 2^30 window (validate without clipping); narrow it to the tile box when clipping.
            Float64 lo_x = -max_wagyu_coordinate;
            Float64 lo_y = -max_wagyu_coordinate;
            Float64 hi_x = max_wagyu_coordinate;
            Float64 hi_y = max_wagyu_coordinate;
            if (clip)
            {
                lo_x = bg::get<bg::min_corner, 0>(box);
                lo_y = bg::get<bg::min_corner, 1>(box);
                hi_x = bg::get<bg::max_corner, 0>(box);
                hi_y = bg::get<bg::max_corner, 1>(box);
            }

            namespace wg = mapbox::geometry::wagyu;
            wg::wagyu<Int64> clipper;
            auto add_clipped = [&](const Ring<BPoint> & ring)
            {
                const Ring<BPoint> clipped = clipRingToBox(ring, lo_x, lo_y, hi_x, hi_y);
                if (clipped.size() >= 3)
                    clipper.add_ring(toWagyuRing(clipped), wg::polygon_type_subject);
            };
            for (const auto & poly : polygons)
            {
                add_clipped(poly.outer());
                for (const auto & inner : poly.inners())
                    add_clipped(inner);
            }

            const auto box_lo_x = static_cast<Int64>(lo_x);
            const auto box_lo_y = static_cast<Int64>(lo_y);
            const auto box_hi_x = static_cast<Int64>(hi_x);
            const auto box_hi_y = static_cast<Int64>(hi_y);
            clipper.add_ring(
                WagyuRing{{box_lo_x, box_lo_y}, {box_hi_x, box_lo_y}, {box_hi_x, box_hi_y}, {box_lo_x, box_hi_y}, {box_lo_x, box_lo_y}},
                wg::polygon_type_clip);

            mapbox::geometry::multi_polygon<Int64> solution;
            clipper.execute(wg::clip_type_intersection, solution, wg::fill_type_even_odd, wg::fill_type_even_odd);

            MultiPolygon<BPoint> result = fromWagyu(solution);
            if (bg::num_points(result) == 0)
            {
                builder.addNull();
                return;
            }
            bg::correct(result);
            builder.addMultiPolygon(result);
#else
            if (clip)
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "Clipping polygons in MVTEncodeGeom requires the wagyu library, which is disabled in this build; "
                    "pass clip = false to project the polygon without clipping");
            /// Without wagyu the unclipped path still emits the projected geometry, repairing only ring orientation
            /// and closure (self-intersections are not repaired - that needs wagyu).
            bg::correct(polygons);
            builder.addMultiPolygon(polygons);
#endif
        };

        /// Dispatch a single Boost geometry value (the right overload is chosen by type).
        auto dispatch = [&](const BPoint & g, const Projection & pr, const BBox & b, bool c) { process_point(g, pr, b, c); };
        auto dispatch_line = [&](const LineString<BPoint> & g, const Projection & pr, const BBox & b, bool c)
        {
            MultiLineString<BPoint> m;
            m.push_back(g);
            process_lines(std::move(m), pr, b, c);
        };
        auto dispatch_mline = [&](const MultiLineString<BPoint> & g, const Projection & pr, const BBox & b, bool c) { process_lines(g, pr, b, c); };
        auto dispatch_ring = [&](const Ring<BPoint> & g, const Projection & pr, const BBox & b, bool c)
        {
            Polygon<BPoint> poly;
            poly.outer() = g;
            MultiPolygon<BPoint> m;
            m.push_back(std::move(poly));
            process_polygons(std::move(m), pr, b, c);
        };
        auto dispatch_polygon = [&](const Polygon<BPoint> & g, const Projection & pr, const BBox & b, bool c)
        {
            MultiPolygon<BPoint> m;
            m.push_back(g);
            process_polygons(std::move(m), pr, b, c);
        };
        auto dispatch_mpolygon = [&](const MultiPolygon<BPoint> & g, const Projection & pr, const BBox & b, bool c) { process_polygons(g, pr, b, c); };

        const auto geometry_type = removeNullable(arguments[0].type);

        if (geometry_type->getName() == "Geometry")
        {
            const auto & variant = assert_cast<const ColumnVariant &>(*full[0].column);

            /// Convert each present sub-column to Boost once, then index by (discriminator, offset) per row.
            const auto convert_sub = [&](UInt8 disc, auto converter) -> decltype(converter(ColumnPtr{}))
            {
                return converter(variant.getVariantPtrByGlobalDiscriminator(disc));
            };
            auto points = convert_sub(GeoDisc::Point, [](ColumnPtr c) { return c ? ColumnToPointsConverter<BPoint>::convert(c) : VectorWithMemoryTracking<BPoint>{}; });
            auto lines = convert_sub(GeoDisc::LineString, [](ColumnPtr c) { return c ? ColumnToLineStringsConverter<BPoint>::convert(c) : VectorWithMemoryTracking<LineString<BPoint>>{}; });
            auto mlines = convert_sub(GeoDisc::MultiLineString, [](ColumnPtr c) { return c ? ColumnToMultiLineStringsConverter<BPoint>::convert(c) : VectorWithMemoryTracking<MultiLineString<BPoint>>{}; });
            auto rings = convert_sub(GeoDisc::Ring, [](ColumnPtr c) { return c ? ColumnToRingsConverter<BPoint>::convert(c) : VectorWithMemoryTracking<Ring<BPoint>>{}; });
            auto polygons = convert_sub(GeoDisc::Polygon, [](ColumnPtr c) { return c ? ColumnToPolygonsConverter<BPoint>::convert(c) : VectorWithMemoryTracking<Polygon<BPoint>>{}; });
            auto mpolygons = convert_sub(GeoDisc::MultiPolygon, [](ColumnPtr c) { return c ? ColumnToMultiPolygonsConverter<BPoint>::convert(c) : VectorWithMemoryTracking<MultiPolygon<BPoint>>{}; });

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                if (has_nulls && null_rows[row])
                {
                    builder.addNull();
                    continue;
                }
                const UInt8 disc = variant.globalDiscriminatorAt(row);
                if (disc == ColumnVariant::NULL_DISCRIMINATOR)
                {
                    builder.addNull();
                    continue;
                }
                Projection projection; BBox box; bool clip = false;
                row_context(row, projection, box, clip);
                const size_t off = variant.offsetAt(row);
                switch (disc)
                {
                    case GeoDisc::Point: dispatch(points[off], projection, box, clip); break;
                    case GeoDisc::LineString: dispatch_line(lines[off], projection, box, clip); break;
                    case GeoDisc::MultiLineString: dispatch_mline(mlines[off], projection, box, clip); break;
                    case GeoDisc::Ring: dispatch_ring(rings[off], projection, box, clip); break;
                    case GeoDisc::Polygon: dispatch_polygon(polygons[off], projection, box, clip); break;
                    case GeoDisc::MultiPolygon: dispatch_mpolygon(mpolygons[off], projection, box, clip); break;
                    default: builder.addNull(); break;
                }
            }
        }
        else
        {
            callOnGeometryDataType<BPoint>(geometry_type, [&](const auto & converter_type)
            {
                using Converter = std::decay_t<decltype(converter_type)>::Type;
                auto geometries = Converter::convert(full[0].column);
                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    if (has_nulls && null_rows[row])
                    {
                        builder.addNull();
                        continue;
                    }
                    Projection projection; BBox box; bool clip = false;
                    row_context(row, projection, box, clip);
                    if constexpr (std::is_same_v<Converter, ColumnToPointsConverter<BPoint>>)
                        dispatch(geometries[row], projection, box, clip);
                    else if constexpr (std::is_same_v<Converter, ColumnToLineStringsConverter<BPoint>>)
                        dispatch_line(geometries[row], projection, box, clip);
                    else if constexpr (std::is_same_v<Converter, ColumnToMultiLineStringsConverter<BPoint>>)
                        dispatch_mline(geometries[row], projection, box, clip);
                    else if constexpr (std::is_same_v<Converter, ColumnToRingsConverter<BPoint>>)
                        dispatch_ring(geometries[row], projection, box, clip);
                    else if constexpr (std::is_same_v<Converter, ColumnToPolygonsConverter<BPoint>>)
                        dispatch_polygon(geometries[row], projection, box, clip);
                    else if constexpr (std::is_same_v<Converter, ColumnToMultiPolygonsConverter<BPoint>>)
                        dispatch_mpolygon(geometries[row], projection, box, clip);
                }
            });
        }

        return builder.finalize();
    }
};

}

REGISTER_FUNCTION(MVTEncodeGeom)
{
    FunctionDocumentation::Description description = R"(
Projects a geometry given in geographic coordinates (longitude/latitude) into the tile-local pixel space of the
slippy-map tile identified by `zoom`, `tile_x` and `tile_y`, snaps it to the integer pixel grid, optionally clips it
to the tile, and returns the tile-space geometry as a `Geometry`.

The projection is Web Mercator over the full `UInt32` coordinate range; the origin is the tile's top-left corner with the
y axis pointing downwards (the Mapbox Vector Tile convention). When `clip` is enabled (the default) the geometry is
clipped to the tile expanded by `buffer` pixels, and geometries that fall entirely outside become `NULL`. The result is
intended to be passed to the aggregate function `MVTEncode`. This function is the analogue of PostGIS `ST_AsMVTGeom`.

Supported input geometry types are `Point`, `LineString`, `MultiLineString`, `Ring`, `Polygon`, `MultiPolygon` and the
`Geometry` variant.
    )";
    FunctionDocumentation::Syntax syntax = "MVTEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]])";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "Geometry in longitude/latitude degrees.", {"Point", "LineString", "MultiLineString", "Ring", "Polygon", "MultiPolygon", "Geometry"}},
        {"zoom", "Slippy-map zoom level, in the range `[0, 32]`.", {"UInt8"}},
        {"tile_x", "Tile column index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
        {"tile_y", "Tile row index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
        {"extent", "Optional tile extent in pixels per side. Defaults to `4096`.", {"UInt32"}},
        {"buffer", "Optional clip buffer in pixels. Defaults to `1`.", {"UInt32"}},
        {"clip", "Optional flag; when nonzero (default) the geometry is clipped to the tile plus buffer.", {"UInt8"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the tile-space geometry, or `NULL` if it is fully clipped out.", {"Geometry"}};
    FunctionDocumentation::Examples examples = {
        {
            "Project a point into a tile",
            "SELECT MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335)",
            "",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMVTEncodeGeom>(documentation);
    factory.registerAlias("ST_AsMVTGeom", FunctionMVTEncodeGeom::name);
}

}
