#include "config.h"

#if USE_H3

#include <array>
#include <cmath>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

/// Implements the function geoToH3 which takes 3 arguments (latitude, longitude and h3 resolution)
/// and returns h3 index of this point
class FunctionGeoToH3 : public IFunction
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
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isFloat64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arg->getName(), 1, getName());

        arg = arguments[1].get();
        if (!WhichDataType(arg).isFloat64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arg->getName(), 2, getName());

        arg = arguments[2].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt8",
                arg->getName(), 3, getName());

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const ColumnFloat64 * col_lat = nullptr;
        const ColumnFloat64 * col_lon = nullptr;

        if (geotoh3_argument_order == GeoToH3ArgumentOrder::LON_LAT)
        {
            col_lon = checkAndGetColumn<ColumnFloat64>(non_const_arguments[0].column.get());
            col_lat = checkAndGetColumn<ColumnFloat64>(non_const_arguments[1].column.get());
        }
        else
        {
            col_lat = checkAndGetColumn<ColumnFloat64>(non_const_arguments[0].column.get());
            col_lon = checkAndGetColumn<ColumnFloat64>(non_const_arguments[1].column.get());
        }

        if (!col_lat)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be Float64.",
                arguments[1].type->getName(),
                2,
                getName());

        const auto & data_lat = col_lat->getData();

        if (!col_lon)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be Float64.",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_lon = col_lon->getData();

        const auto * col_res = checkAndGetColumn<ColumnUInt8>(non_const_arguments[2].column.get());
        if (!col_res)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be UInt8.",
                    arguments[2].type->getName(),
                    3,
                    getName());
        const auto & data_res = col_res->getData();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const double lon = data_lon[row];
            const double lat = data_lat[row];
            const UInt8 res = data_res[row];

            if (res > MAX_H3_RES)
                throw Exception(
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                        toString(res),
                        getName(),
                        MAX_H3_RES);

            LatLng coord;
            coord.lng = degsToRads(lon);
            coord.lat = degsToRads(lat);

            H3Index hindex;
            H3Error err = latLngToCell(&coord, res, &hindex);
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

:::note
In ClickHouse v25.4 or older, `geoToH3()` arguments are in the order `(lon, lat)`. As per ClickHouse v25.5, the input values are ordered `(lat, lon)`.
The previous behavior can be restored using setting `geotoh3_argument_order = 'lon_lat'`.
    )";
    FunctionDocumentation::Syntax syntax = "geoToH3(lat, lon, resolution)";
    FunctionDocumentation::Arguments arguments = {
        {"lat", "Latitude in degrees.", {"Float64"}},
        {"lon", "Longitude in degrees.", {"Float64"}},
        {"resolution", "Index resolution with range `[0, 15]`.", {"UInt8"}}
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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionGeoToH3>(documentation);
}

}

#endif

