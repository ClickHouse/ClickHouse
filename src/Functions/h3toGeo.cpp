#include "config.h"

#if USE_H3

#include <array>
#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

#include <h3api.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool h3togeo_lon_lat_result_order;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Implements the function h3ToGeo which takes a single argument (h3Index)
/// and returns the longitude and latitude that correspond to the provided h3 index
class FunctionH3ToGeo : public IFunction
{
    const bool h3togeo_lon_lat_result_order;
public:
    static constexpr auto name = "h3ToGeo";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionH3ToGeo>(context); }

    explicit FunctionH3ToGeo(ContextPtr context)
        : h3togeo_lon_lat_result_order(context->getSettingsRef()[Setting::h3togeo_lon_lat_result_order])
    {
    }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        if (h3togeo_lon_lat_result_order)
        {
            return std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
                Strings{"longitude", "latitude"});
        }
        else
        {
            return std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
                Strings{"latitude", "longitude"});
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * column = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data = column->getData();

        auto latitude = ColumnFloat64::create(input_rows_count);
        auto longitude = ColumnFloat64::create(input_rows_count);

        ColumnFloat64::Container & lon_data = longitude->getData();
        ColumnFloat64::Container & lat_data = latitude->getData();


        for (size_t row = 0; row < input_rows_count; ++row)
        {
            H3Index h3index = data[row];
            LatLng coord{};

            cellToLatLng(h3index,&coord);
            lon_data[row] = radsToDegs(coord.lng);
            lat_data[row] = radsToDegs(coord.lat);
        }

        MutableColumns columns;
        if (h3togeo_lon_lat_result_order)
        {
            columns.emplace_back(std::move(longitude));
            columns.emplace_back(std::move(latitude));
        }
        else
        {
            columns.emplace_back(std::move(latitude));
            columns.emplace_back(std::move(longitude));
        }
        return ColumnTuple::create(std::move(columns));
    }
};

}

REGISTER_FUNCTION(H3ToGeo)
{
    FunctionDocumentation::Description description = R"(
Returns the centroid latitude and longitude corresponding to the provided [H3](https://h3geo.org/docs/core-library/h3Indexing/) index.

:::note
In ClickHouse v24.12 or older, `h3ToGeo()` accepts arguments in the order `(lon, lat)`. As per ClickHouse v25.1, the returned values are ordered `(lat, lon)`.
The previous behavior can be restored using setting `h3togeo_lon_lat_result_order = true`.
:::
    )";
    FunctionDocumentation::Syntax syntax = "h3ToGeo(h3Index)";
    FunctionDocumentation::Arguments arguments = {
        {"h3Index", "H3 index.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a tuple consisting of two values `(lat, lon)` where `lat` is latitude and `lon` is longitude.",
        {"Tuple(Float64, Float64)"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get coordinates from H3 index",
            "SELECT h3ToGeo(644325524701193974) AS coordinates",
            R"(
┌─coordinates───────────────────────────┐
│ (55.71290243145668,37.79506616830252) │
└───────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3ToGeo>(documentation);
}

}

#endif
