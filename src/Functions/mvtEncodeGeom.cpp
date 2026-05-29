#include <cmath>
#include <numbers>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

/// mvtEncodeGeom(longitude, latitude, zoom, tile_x, tile_y[, extent]) projects a (lon, lat) point into the
/// tile-local pixel space of slippy-map tile (zoom, tile_x, tile_y) and returns it as a Tuple(Float64, Float64).
///
/// The projection is Web Mercator over the full UInt32 range, identical to the materialized `mercator_x`/`mercator_y`
/// columns used by tile-serving schemas. The result origin is the tile's top-left corner with the y axis pointing
/// down, matching the Mapbox Vector Tile coordinate convention, so the output feeds directly into `mvtEncode`.
/// Coordinates are rounded so that `GROUP BY mvtEncodeGeom(...)` collapses points sharing a pixel into one cluster.
class FunctionMvtEncodeGeom final : public IFunction
{
public:
    static constexpr auto name = "mvtEncodeGeom";

    /// 0xFFFFFFFF: the Web Mercator projection spans the full UInt32 range.
    static constexpr double mercator_max = 4294967295.0;
    /// Latitude bound of the Web Mercator projection (the projection diverges at the poles).
    static constexpr double latitude_limit = 85.05112877980659;
    static constexpr UInt64 default_extent = 4096;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMvtEncodeGeom>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"longitude", &isNumber, nullptr, "Float64"},
            {"latitude", &isNumber, nullptr, "Float64"},
            {"zoom", &isNativeUInt, nullptr, "UInt8"},
            {"tile_x", &isNativeUInt, nullptr, "UInt32"},
            {"tile_y", &isNativeUInt, nullptr, "UInt32"},
        };
        FunctionArgumentDescriptors optional_args{
            {"extent", &isNativeUInt, nullptr, "UInt32"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto full_arguments = arguments;
        for (auto & argument : full_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const IColumn & col_lon = *full_arguments[0].column;
        const IColumn & col_lat = *full_arguments[1].column;
        const IColumn & col_zoom = *full_arguments[2].column;
        const IColumn & col_tile_x = *full_arguments[3].column;
        const IColumn & col_tile_y = *full_arguments[4].column;
        const bool has_extent = full_arguments.size() == 6;
        const IColumn * col_extent = has_extent ? full_arguments[5].column.get() : nullptr;

        auto col_pixel_x = ColumnFloat64::create(input_rows_count);
        auto col_pixel_y = ColumnFloat64::create(input_rows_count);
        auto & pixel_x_data = col_pixel_x->getData();
        auto & pixel_y_data = col_pixel_y->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 zoom = col_zoom.getUInt(row);
            if (zoom > 32)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The 'zoom' argument ({}) of function {} must be between 0 and 32",
                    zoom,
                    getName());

            const UInt64 extent = has_extent ? col_extent->getUInt(row) : default_extent;
            if (extent == 0)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The 'extent' argument of function {} must be positive", getName());

            const UInt64 tile_x = col_tile_x.getUInt(row);
            const UInt64 tile_y = col_tile_y.getUInt(row);
            const UInt64 num_tiles = UInt64{1} << zoom;
            if (tile_x >= num_tiles || tile_y >= num_tiles)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The tile indices (tile_x = {}, tile_y = {}) of function {} must be less than 2^zoom = {}",
                    tile_x,
                    tile_y,
                    getName(),
                    num_tiles);

            const double longitude = std::clamp(col_lon.getFloat64(row), -180.0, 180.0);
            const double latitude = std::clamp(col_lat.getFloat64(row), -latitude_limit, latitude_limit);

            const double mercator_x = mercator_max * (longitude + 180.0) / 360.0;
            const double mercator_y
                = mercator_max * (0.5 - std::log(std::tan((latitude + 90.0) / 360.0 * std::numbers::pi)) / (2.0 * std::numbers::pi));

            /// The full UInt32 Mercator space is divided into 2^zoom tiles per axis; tile_size is the side of one tile.
            const double tile_size = std::exp2(static_cast<double>(32 - zoom));
            const double tile_x_begin = static_cast<double>(tile_x) * tile_size;
            const double tile_y_begin = static_cast<double>(tile_y) * tile_size;

            const double scale = static_cast<double>(extent) / tile_size;
            pixel_x_data[row] = std::round((mercator_x - tile_x_begin) * scale);
            pixel_y_data[row] = std::round((mercator_y - tile_y_begin) * scale);
        }

        return ColumnTuple::create(Columns{std::move(col_pixel_x), std::move(col_pixel_y)});
    }
};

}

REGISTER_FUNCTION(MvtEncodeGeom)
{
    FunctionDocumentation::Description description = R"(
Projects a geographic point given by `longitude` and `latitude` into the tile-local pixel space of the slippy-map
tile identified by `zoom`, `tile_x` and `tile_y`, and returns the pixel coordinates as a `Tuple(Float64, Float64)`.

The projection is Web Mercator over the full `UInt32` coordinate range. The returned coordinates have their origin at
the top-left corner of the tile with the y axis pointing downwards, which is the coordinate convention of the
[Mapbox Vector Tile](https://github.com/mapbox/vector-tile-spec) format. The result is intended to be passed directly
to the aggregate function `mvtEncode`. Coordinates are rounded to whole pixels, so grouping by `mvtEncodeGeom` collapses
all points falling on the same pixel into a single cluster.

This function transforms point geometry only and does not clip to the tile boundary; restrict rows to the tile in the
`WHERE` clause (for example with a Mercator bounding-box predicate).
    )";
    FunctionDocumentation::Syntax syntax = "mvtEncodeGeom(longitude, latitude, zoom, tile_x, tile_y[, extent])";
    FunctionDocumentation::Arguments arguments = {
        {"longitude", "Longitude in degrees. Values are clamped to the range `[-180, 180]`.", {"Float64"}},
        {"latitude", "Latitude in degrees. Values are clamped to the Web Mercator range `[-85.05112878, 85.05112878]`.", {"Float64"}},
        {"zoom", "Slippy-map zoom level, in the range `[0, 32]`.", {"UInt8"}},
        {"tile_x", "Tile column index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
        {"tile_y", "Tile row index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
        {"extent", "Optional tile extent in pixels per side. Defaults to `4096` (the Mapbox Vector Tile default).", {"UInt32"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the tile-local pixel coordinates as a tuple `(pixel_x, pixel_y)`.", {"Tuple(Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Project a point into a tile",
            "SELECT mvtEncodeGeom(13.37, 52.52, 10, 550, 335) AS pixel",
            R"(
┌─pixel──────┐
│ (124,3384) │
└────────────┘
            )",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMvtEncodeGeom>(documentation);
}

}
