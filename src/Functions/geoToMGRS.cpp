#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UTMCoordinates.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/NaNUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// geoToMGRS(longitude, latitude[, precision]) => String
class FunctionGeoToMGRS final : public IFunction
{
public:
    static constexpr auto name = "geoToMGRS";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoToMGRS>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"longitude", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "Float*"},
            {"latitude", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "Float*"}};
        FunctionArgumentDescriptors optional_args{
            {"precision", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "(U)Int*"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * lon_column = arguments[0].column.get();
        const IColumn * lat_column = arguments[1].column.get();
        const IColumn * precision_column = arguments.size() >= 3 ? arguments[2].column.get() : nullptr;

        auto col_str = ColumnString::create();
        col_str->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const Float64 lon = lon_column->getFloat64(i);
            const Float64 lat = lat_column->getFloat64(i);

            if (!isFinite(lon) || !isFinite(lat))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} must be finite numbers", getName());

            if (lat < -80.0 || lat > 84.0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Latitude {} is out of the UTM domain [-80, 84] in function {}", lat, getName());
            if (lon < -180.0 || lon > 180.0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Longitude {} is out of range [-180, 180] in function {}", lon, getName());

            UInt8 precision = 5;
            if (precision_column)
            {
                const Int64 value = precision_column->getInt(i);
                if (value < 0 || value > 5)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "Precision {} is out of range [0, 5] in function {}", value, getName());
                precision = static_cast<UInt8>(value);
            }

            const std::string mgrs = mgrsEncode(lon, lat, precision);
            col_str->insertData(mgrs.data(), mgrs.size());
        }

        return col_str;
    }
};

}

REGISTER_FUNCTION(GeoToMGRS)
{
    FunctionDocumentation::Description description = R"(
Encodes WGS84 geographic coordinates (longitude, latitude) as a [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) string.

The string is `<zone><band><100km square><easting><northing>`, for example `31UDQ4825111935`. The `precision` argument controls the number of digits used for each of the easting and northing: `5` (default) for 1 m, `4` for 10 m, down to `0` for the 100 km grid square only. MGRS is only defined for latitudes in the range `[-80°, 84°]`.
    )";
    FunctionDocumentation::Syntax syntax = "geoToMGRS(longitude, latitude[, precision])";
    FunctionDocumentation::Arguments arguments = {
        {"longitude", "Longitude in degrees. Range: `[-180°, 180°]`.", {"Float32", "Float64"}},
        {"latitude", "Latitude in degrees. Range: `[-80°, 84°]`.", {"Float32", "Float64"}},
        {"precision", "Optional. Number of digits for each of easting and northing. Default: 5. Range: `[0, 5]`.", {"(U)Int*"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the MGRS reference string.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT geoToMGRS(2.294497, 48.858222)", "31UDQ4825111935"},
        {"Lower precision (100 m)", "SELECT geoToMGRS(2.294497, 48.858222, 3)", "31UDQ482119"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGeoToMGRS>(documentation);
}

}
