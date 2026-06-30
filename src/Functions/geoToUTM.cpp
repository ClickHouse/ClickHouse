#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UTMCoordinates.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/NaNUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// geoToUTM(longitude, latitude[, zone]) => (easting Float64, northing Float64, zone UInt8, band FixedString(1))
class FunctionGeoToUTM final : public IFunction
{
public:
    static constexpr auto name = "geoToUTM";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoToUTM>(); }

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
            {"zone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "(U)Int*"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeTuple>(
            DataTypes{
                std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeFixedString>(1)},
            Strings{"easting", "northing", "zone", "band"});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * lon_column = arguments[0].column.get();
        const IColumn * lat_column = arguments[1].column.get();
        const IColumn * zone_column = arguments.size() >= 3 ? arguments[2].column.get() : nullptr;

        auto col_easting = ColumnFloat64::create(input_rows_count);
        auto col_northing = ColumnFloat64::create(input_rows_count);
        auto col_zone = ColumnUInt8::create(input_rows_count);
        auto col_band = ColumnFixedString::create(1);
        col_band->getChars().resize(input_rows_count);

        auto & easting_data = col_easting->getData();
        auto & northing_data = col_northing->getData();
        auto & zone_data = col_zone->getData();
        auto & band_chars = col_band->getChars();

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

            UInt8 forced_zone = 0;
            if (zone_column)
            {
                const Int64 zone = zone_column->getInt(i);
                if (zone < 1 || zone > 60)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "UTM zone {} is out of range [1, 60] in function {}", zone, getName());
                forced_zone = static_cast<UInt8>(zone);
            }

            const UTMCoordinate utm = wgs84ToUTM(lon, lat, forced_zone);
            easting_data[i] = utm.easting;
            northing_data[i] = utm.northing;
            zone_data[i] = utm.zone;
            band_chars[i] = static_cast<UInt8>(utm.band);
        }

        return ColumnTuple::create(
            Columns{std::move(col_easting), std::move(col_northing), std::move(col_zone), std::move(col_band)});
    }
};

}

REGISTER_FUNCTION(GeoToUTM)
{
    FunctionDocumentation::Description description = R"(
Converts WGS84 geographic coordinates (longitude, latitude) to [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) coordinates.

The zone is selected automatically from the longitude, applying the standard exceptions for Norway and Svalbard, unless an explicit `zone` is given. UTM is only defined for latitudes in the range `[-80°, 84°]`.
    )";
    FunctionDocumentation::Syntax syntax = "geoToUTM(longitude, latitude[, zone])";
    FunctionDocumentation::Arguments arguments = {
        {"longitude", "Longitude in degrees. Range: `[-180°, 180°]`.", {"Float32", "Float64"}},
        {"latitude", "Latitude in degrees. Range: `[-80°, 84°]`.", {"Float32", "Float64"}},
        {"zone", "Optional. Force projection into this UTM zone instead of selecting it automatically. Range: `[1, 60]`.", {"(U)Int*"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a named tuple `(easting, northing, zone, band)`: easting and northing in metres, the UTM zone number, and the MGRS latitude band letter (band `>= 'N'` is the northern hemisphere).",
        {"Tuple(Float64, Float64, UInt8, FixedString(1))"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT geoToUTM(2.294497, 48.858222)", "(448251.5978370684,5411935.125629659,31,'U')"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGeoToUTM>(documentation);
}

}
