#include "config.h"

#if USE_H3

#include <array>
#include <cmath>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <base/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace Setting
{
    extern const SettingsGeoToH3ArgumentOrder geotoh3_argument_order;
}
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

/// Implements the function geoToH3 which takes 3 arguments (latitude, longitude and h3 resolution)
/// and returns h3 index of this point
class FunctionGeoToH3 final : public IFunction
{
    GeoToH3ArgumentOrder geotoh3_argument_order;
public:
    static constexpr auto name = "geoToH3";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGeoToH3>(context); }

    explicit FunctionGeoToH3(ContextPtr context)
    : geotoh3_argument_order(context->getSettingsRef()[Setting::geotoh3_argument_order])
    {
    }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

    String getSignatureString() const override
    {
        return "(Number, Number, Integer) -> UInt64";
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        const auto float64_type = std::make_shared<DataTypeFloat64>();
        for (auto & argument : non_const_arguments)
        {
            /// Cast constant inputs before expanding them for row-wise access.
            argument.column = castColumn(argument, float64_type)->convertToFullColumnIfConst();
        }

        const size_t lat_arg = geotoh3_argument_order == GeoToH3ArgumentOrder::LON_LAT ? 1 : 0;
        const size_t lon_arg = geotoh3_argument_order == GeoToH3ArgumentOrder::LON_LAT ? 0 : 1;

        const auto & data_lat = assert_cast<const ColumnFloat64 &>(*non_const_arguments[lat_arg].column).getData();
        const auto & data_lon = assert_cast<const ColumnFloat64 &>(*non_const_arguments[lon_arg].column).getData();
        const auto & data_res = assert_cast<const ColumnFloat64 &>(*non_const_arguments[2].column).getData();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const double lon = data_lon[row];
            const double lat = data_lat[row];
            /// Valid integer resolutions are exactly representable as Float64.
            /// Check the range before converting to the narrow type required by H3.
            const double resolution = data_res[row];
            if (resolution < 0 || resolution > MAX_H3_RES)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                    resolution,
                    getName(),
                    MAX_H3_RES);

            if (lat < -90.0 || lat > 90.0)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'latitude' ({}) of function {} is out of bounds because the latitude must be in the range [-90, 90]",
                    lat,
                    getName());

            LatLng coord;
            coord.lng = degsToRads(lon);
            coord.lat = degsToRads(lat);

            H3Index hindex = 0;
            H3Error err = latLngToCell(&coord, static_cast<UInt8>(resolution), &hindex);
            if (err)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect coordinates latitude: {}, longitude: {}, error: {}", coord.lat, coord.lng, err);

            dst_data[row] = hindex;
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(GeoToH3)
{
    FunctionDocumentation::Description description = R"(
Returns [H3](https://h3geo.org/docs/core-library/h3Indexing/) point index for the given latitude, longitude, and resolution.

<Note>
In ClickHouse v25.4 or older, `geoToH3()` arguments are in the order `(lon, lat)`. As per ClickHouse v25.5, the input values are ordered `(lat, lon)`.
The previous behavior can be restored using setting `geotoh3_argument_order = 'lon_lat'`.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "geoToH3(lat, lon, resolution)";
    FunctionDocumentation::Arguments arguments = {
        {"lat", "Latitude in degrees.", {"(U)Int*", "Float*", "Decimal"}},
        {"lon", "Longitude in degrees.", {"(U)Int*", "Float*", "Decimal"}},
        {"resolution", "Index resolution with range `[0, 15]`.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the H3 index number, or `0` in case of error.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Convert coordinates to H3 index",
            "SELECT geoToH3(55.71290588, 37.79506683, 15) AS h3Index",
            R"(
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionGeoToH3>(documentation);
}

}

#endif
