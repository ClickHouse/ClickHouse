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

/// 0xFFFFFFFF: the Web Mercator projection used by `mvtEncodeGeom` spans the full UInt32 range.
constexpr double mercator_max = 4294967295.0;
/// Latitude bound of the Web Mercator projection (the projection diverges at the poles).
constexpr double latitude_limit = 85.05112877980659;

/// Which coordinate space the bounding box is reported in.
enum class BBoxSpace : UInt8
{
    Geographic, /// (min_lon, min_lat, max_lon, max_lat) in degrees.
    Mercator, /// (min_x, min_y, max_x, max_y) in the full-UInt32 Web Mercator space.
};

double mercatorXToLongitude(double mercator_x)
{
    return mercator_x / mercator_max * 360.0 - 180.0;
}

double mercatorYToLatitude(double mercator_y)
{
    /// Inverse of the forward projection in `mvtEncodeGeom`: mercator_y = max * (0.5 - ln(tan((lat+90)/360*pi)) / (2*pi)).
    const double t = 0.5 - mercator_y / mercator_max;
    return std::atan(std::exp(2.0 * std::numbers::pi * t)) * 360.0 / std::numbers::pi - 90.0;
}

/// mvtTileBBox / mvtTileBBoxMercator: the bounding box of a slippy-map tile (zoom, tile_x, tile_y), optionally expanded
/// by `margin` (a fraction of the tile size) on every side. The geographic form returns (min_lon, min_lat, max_lon,
/// max_lat) in degrees; the Mercator form returns (min_x, min_y, max_x, max_y) in the full-UInt32 Web Mercator space
/// used internally by `mvtEncodeGeom`. The box is the natural companion to `mvtEncodeGeom`'s clip mode: use it in the
/// `WHERE` clause to restrict rows to the tile, with `margin = buffer / extent` to cover the clip buffer zone.
template <BBoxSpace space>
class FunctionMvtTileBBox final : public IFunction
{
public:
    static constexpr auto name = space == BBoxSpace::Geographic ? "mvtTileBBox" : "mvtTileBBoxMercator";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMvtTileBBox>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"zoom", &isNativeUInt, nullptr, "UInt8"},
            {"tile_x", &isNativeUInt, nullptr, "UInt32"},
            {"tile_y", &isNativeUInt, nullptr, "UInt32"},
        };
        FunctionArgumentDescriptors optional_args{
            {"margin", &isNumber, nullptr, "Float64"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        const Strings element_names = space == BBoxSpace::Geographic
            ? Strings{"min_lon", "min_lat", "max_lon", "max_lat"}
            : Strings{"min_x", "min_y", "max_x", "max_y"};

        return std::make_shared<DataTypeTuple>(
            DataTypes{
                std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeFloat64>()},
            element_names);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto full_arguments = arguments;
        for (auto & argument : full_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const IColumn & col_zoom = *full_arguments[0].column;
        const IColumn & col_tile_x = *full_arguments[1].column;
        const IColumn & col_tile_y = *full_arguments[2].column;
        const bool has_margin = full_arguments.size() == 4;
        const IColumn * col_margin = has_margin ? full_arguments[3].column.get() : nullptr;

        auto col_a = ColumnFloat64::create(input_rows_count);
        auto col_b = ColumnFloat64::create(input_rows_count);
        auto col_c = ColumnFloat64::create(input_rows_count);
        auto col_d = ColumnFloat64::create(input_rows_count);
        auto & data_a = col_a->getData();
        auto & data_b = col_b->getData();
        auto & data_c = col_c->getData();
        auto & data_d = col_d->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 zoom = col_zoom.getUInt(row);
            if (zoom > 32)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The 'zoom' argument ({}) of function {} must be between 0 and 32",
                    zoom,
                    getName());

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

            const double tile_size = std::exp2(static_cast<double>(32 - zoom));
            const double margin = has_margin ? col_margin->getFloat64(row) : 0.0;
            const double pad = margin * tile_size;

            /// The full UInt32 Mercator space; y grows downward (north at the top), matching `mvtEncodeGeom`.
            const double min_x = std::clamp(static_cast<double>(tile_x) * tile_size - pad, 0.0, mercator_max);
            const double max_x = std::clamp((static_cast<double>(tile_x) + 1.0) * tile_size + pad, 0.0, mercator_max);
            const double min_y = std::clamp(static_cast<double>(tile_y) * tile_size - pad, 0.0, mercator_max);
            const double max_y = std::clamp((static_cast<double>(tile_y) + 1.0) * tile_size + pad, 0.0, mercator_max);

            if constexpr (space == BBoxSpace::Geographic)
            {
                /// Smaller mercator_y is further north (higher latitude), so min_y gives max_lat and vice versa.
                data_a[row] = std::clamp(mercatorXToLongitude(min_x), -180.0, 180.0);
                data_b[row] = std::clamp(mercatorYToLatitude(max_y), -latitude_limit, latitude_limit);
                data_c[row] = std::clamp(mercatorXToLongitude(max_x), -180.0, 180.0);
                data_d[row] = std::clamp(mercatorYToLatitude(min_y), -latitude_limit, latitude_limit);
            }
            else
            {
                data_a[row] = min_x;
                data_b[row] = min_y;
                data_c[row] = max_x;
                data_d[row] = max_y;
            }
        }

        return ColumnTuple::create(Columns{std::move(col_a), std::move(col_b), std::move(col_c), std::move(col_d)});
    }
};

}

REGISTER_FUNCTION(MvtTileBBox)
{
    {
        FunctionDocumentation::Description description = R"(
Returns the geographic bounding box of the slippy-map tile identified by `zoom`, `tile_x` and `tile_y` as a tuple
`(min_lon, min_lat, max_lon, max_lat)` in degrees.

The bounding box is the companion of `mvtEncodeGeom`: use it in a `WHERE` clause to restrict rows to a tile while
filtering on the `longitude`/`latitude` columns directly (so a primary key or index on those columns can be used),
instead of recomputing the Web Mercator projection per row. The optional `margin` expands the box on every side by that
fraction of the tile size; set `margin` to `buffer / extent` to match the clip buffer of `mvtEncodeGeom`.
        )";
        FunctionDocumentation::Syntax syntax = "mvtTileBBox(zoom, tile_x, tile_y[, margin])";
        FunctionDocumentation::Arguments arguments = {
            {"zoom", "Slippy-map zoom level, in the range `[0, 32]`.", {"UInt8"}},
            {"tile_x", "Tile column index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
            {"tile_y", "Tile row index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
            {"margin", "Optional fraction of the tile size to expand the box on every side. Defaults to `0`.", {"Float64"}},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = {"Returns the tile bounding box as a tuple `(min_lon, min_lat, max_lon, max_lat)` in degrees.",
               {"Tuple(Float64, Float64, Float64, Float64)"}};
        FunctionDocumentation::Examples examples = {
            {
                "Bounding box of the whole world at zoom 0",
                "SELECT mvtTileBBox(0, 0, 0) AS bbox",
                R"(
┌─bbox───────────────────────────────────────────┐
│ (-180,-85.05112877980659,180,85.05112877980659) │
└─────────────────────────────────────────────────┘
                )",
            },
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionMvtTileBBox<BBoxSpace::Geographic>>(documentation);
    }

    {
        FunctionDocumentation::Description description = R"(
Returns the bounding box of the slippy-map tile identified by `zoom`, `tile_x` and `tile_y` in the full-`UInt32` Web
Mercator coordinate space used internally by `mvtEncodeGeom`, as a tuple `(min_x, min_y, max_x, max_y)`.

This is the Web Mercator counterpart of `mvtTileBBox`, intended for tables that materialize Mercator coordinate columns
and index those instead of `longitude`/`latitude`. The y axis grows downward (north at the top), matching `mvtEncodeGeom`.
The optional `margin` expands the box on every side by that fraction of the tile size.
        )";
        FunctionDocumentation::Syntax syntax = "mvtTileBBoxMercator(zoom, tile_x, tile_y[, margin])";
        FunctionDocumentation::Arguments arguments = {
            {"zoom", "Slippy-map zoom level, in the range `[0, 32]`.", {"UInt8"}},
            {"tile_x", "Tile column index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
            {"tile_y", "Tile row index, in the range `[0, 2^zoom - 1]`.", {"UInt32"}},
            {"margin", "Optional fraction of the tile size to expand the box on every side. Defaults to `0`.", {"Float64"}},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = {"Returns the tile bounding box as a tuple `(min_x, min_y, max_x, max_y)` in Web Mercator coordinates.",
               {"Tuple(Float64, Float64, Float64, Float64)"}};
        FunctionDocumentation::Examples examples = {
            {
                "Mercator bounding box of a tile",
                "SELECT mvtTileBBoxMercator(1, 0, 0) AS bbox",
                R"(
┌─bbox────────────────────────┐
│ (0,0,2147483648,2147483648) │
└─────────────────────────────┘
                )",
            },
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionMvtTileBBox<BBoxSpace::Mercator>>(documentation);
    }
}

}
