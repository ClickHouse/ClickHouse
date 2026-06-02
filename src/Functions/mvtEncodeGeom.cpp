#include <cmath>
#include <limits>
#include <numbers>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>

#include <Columns/ColumnVariant.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeFactory.h>
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

/// 0xFFFFFFFF: the Web Mercator projection spans the full UInt32 range (matching the materialized mercator_x/y columns).
constexpr double mercator_max = 4294967295.0;
/// Latitude bound of the Web Mercator projection (it diverges at the poles).
constexpr double latitude_limit = 85.05112877980659;

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
/// Rounds to the nearest integer and normalises negative zero to positive zero so coordinates clipped to an axis
/// do not surface as "-0".
inline Float64 roundCoordinate(Float64 value)
{
    const Float64 rounded = std::round(value);
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

/// mvtEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]]) -> Geometry
///
/// Projects a geometry (in geographic lon/lat) into the tile-local pixel space of the slippy-map tile identified by
/// zoom/tile_x/tile_y, optionally clips it to the tile expanded by `buffer` pixels, snaps to the integer pixel grid, and
/// returns the tile-space geometry as a `Geometry` (NULL when the geometry falls entirely outside the clip window). The
/// result feeds the aggregate function `mvtEncode`. Analogue of PostGIS `ST_AsMVTGeom` (registered alias `ST_AsMVTGeom`).
class FunctionMvtEncodeGeom final : public IFunction
{
public:
    static constexpr auto name = "mvtEncodeGeom";

    static constexpr UInt64 default_extent = 4096;
    static constexpr UInt64 default_buffer = 256;
    static constexpr Int64 max_extent_or_buffer = std::numeric_limits<Int32>::max();

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMvtEncodeGeom>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 4 || arguments.size() > 7)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} requires 4 to 7 arguments (geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]]), got {}",
                getName(),
                arguments.size());

        const auto & geometry_type = arguments[0].type;
        if (geometry_type->getName() != "Geometry" && !getGeometryColumnTypeFromDataType(geometry_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} must be a geometry (Point, LineString, MultiLineString, Ring, Polygon, "
                "MultiPolygon or Geometry), got {}",
                getName(),
                geometry_type->getName());

        for (size_t i = 1; i < arguments.size(); ++i)
            if (!isNumber(arguments[i].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument #{} of function {} must be numeric, got {}",
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
            clip = has_clip ? (col_clip->getUInt(row) != 0) : true;
            const double lo = -static_cast<double>(buffer);
            const double hi = static_cast<double>(extent) + static_cast<double>(buffer);
            box = BBox(BPoint(lo, lo), BPoint(hi, hi));
        };

        /// Process one geometry of a concrete Boost type into the result builder.
        auto process_point = [&](BPoint p, const Projection & projection, const BBox & box, bool clip)
        {
            projection(p);
            if (clip && !bg::covered_by(p, box))
            {
                builder.addNull();
                return;
            }
            bg::set<0>(p, roundCoordinate(bg::get<0>(p)));
            bg::set<1>(p, roundCoordinate(bg::get<1>(p)));
            builder.addPoint(p);
        };

        auto process_lines = [&](MultiLineString<BPoint> lines, const Projection & projection, const BBox & box, bool clip)
        {
            bg::for_each_point(lines, projection);
            MultiLineString<BPoint> result;
            if (clip)
                bg::intersection(lines, box, result);
            else
                result = std::move(lines);
            if (result.empty())
            {
                builder.addNull();
                return;
            }
            roundToGrid(result);
            builder.addMultiLineString(result);
        };

        auto process_polygons = [&](MultiPolygon<BPoint> polygons, const Projection & projection, const BBox & box, bool clip)
        {
            bg::for_each_point(polygons, projection);
            bg::correct(polygons);
            MultiPolygon<BPoint> result;
            if (clip)
                bg::intersection(polygons, box, result);
            else
                result = std::move(polygons);
            if (result.empty())
            {
                builder.addNull();
                return;
            }
            bg::correct(result);
            roundToGrid(result);
            builder.addMultiPolygon(result);
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

        const auto & geometry_type = arguments[0].type;

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

REGISTER_FUNCTION(MvtEncodeGeom)
{
    FunctionDocumentation::Description description = R"(
Projects a geometry given in geographic coordinates (longitude/latitude) into the tile-local pixel space of the
slippy-map tile identified by `zoom`, `tile_x` and `tile_y`, optionally clips it to the tile, snaps it to the integer
pixel grid, and returns the tile-space geometry as a `Geometry`.

The projection is Web Mercator over the full `UInt32` coordinate range; the origin is the tile's top-left corner with the
y axis pointing downwards (the Mapbox Vector Tile convention). When `clip` is enabled (the default) the geometry is
clipped to the tile expanded by `buffer` pixels, and geometries that fall entirely outside become `NULL`. The result is
intended to be passed to the aggregate function `mvtEncode`. This function is the analogue of PostGIS `ST_AsMVTGeom`.

Supported input geometry types are `Point`, `LineString`, `MultiLineString`, `Ring`, `Polygon`, `MultiPolygon` and the
`Geometry` variant.
    )";
    FunctionDocumentation::Syntax syntax = "mvtEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]])";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "Geometry in longitude/latitude degrees.", {"Point", "LineString", "Polygon", "MultiPolygon", "Geometry"}},
        {"zoom", "Slippy-map zoom level, in the range `[0, 32]`.", {"UInt8"}},
        {"tile_x", "Tile column index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
        {"tile_y", "Tile row index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
        {"extent", "Optional tile extent in pixels per side. Defaults to `4096`.", {"UInt32"}},
        {"buffer", "Optional clip buffer in pixels. Defaults to `256`.", {"UInt32"}},
        {"clip", "Optional flag; when nonzero (default) the geometry is clipped to the tile plus buffer.", {"UInt8"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the tile-space geometry, or `NULL` if it is fully clipped out.", {"Geometry"}};
    FunctionDocumentation::Examples examples = {
        {
            "Project a point into a tile",
            "SELECT mvtEncodeGeom((13.37, 52.52)::Point, 10, 550, 335)",
            "",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMvtEncodeGeom>(documentation);
    factory.registerAlias("ST_AsMVTGeom", FunctionMvtEncodeGeom::name);
}

}
