#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UTMCoordinates.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/NaNUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool isIntegerOrBandString(const IDataType & type)
{
    return isInteger(type) || isString(type) || isFixedString(type);
}

/// UTMToGeo(easting, northing, zone, is_north) => (longitude Float64, latitude Float64)
class FunctionUTMToGeo final : public IFunction
{
public:
    static constexpr auto name = "UTMToGeo";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUTMToGeo>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"easting", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {"northing", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {"zone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "(U)Int*"},
            {"is_north", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isIntegerOrBandString), nullptr, "(U)Int* (0 or 1) or String/FixedString (MGRS band letter)"}};
        validateFunctionArguments(*this, arguments, mandatory_args);

        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
            Strings{"longitude", "latitude"});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * easting_column = arguments[0].column.get();
        const IColumn * northing_column = arguments[1].column.get();
        const IColumn * zone_column = arguments[2].column.get();
        const IColumn * is_north_column = arguments[3].column.get();
        const bool band_is_string = isString(*arguments[3].type) || isFixedString(*arguments[3].type);

        auto col_longitude = ColumnFloat64::create(input_rows_count);
        auto col_latitude = ColumnFloat64::create(input_rows_count);

        auto & longitude_data = col_longitude->getData();
        auto & latitude_data = col_latitude->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const Float64 easting = easting_column->getFloat64(i);
            const Float64 northing = northing_column->getFloat64(i);

            if (!isFinite(easting) || !isFinite(northing))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Easting and northing arguments of function {} must be finite numbers", getName());

            const Int64 zone = zone_column->getInt(i);
            if (zone < 1 || zone > 60)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "UTM zone {} is out of range [1, 60] in function {}", zone, getName());

            bool is_north;
            if (band_is_string)
            {
                const StringRef band_ref = is_north_column->getDataAt(i);
                if (band_ref.size != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "MGRS band letter argument of function {} must be a single character, got string of length {}",
                        getName(), band_ref.size);
                const char band = static_cast<char>(std::toupper(static_cast<unsigned char>(band_ref.data[0])));
                // Valid MGRS latitude bands: C..X, skipping I and O
                if (band < 'C' || band > 'X' || band == 'I' || band == 'O')
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "MGRS band letter '{}' is not valid in function {}. Valid letters are C..X excluding I and O",
                        band, getName());
                is_north = (band >= 'N');
            }
            else
            {
                const Int64 is_north_value = is_north_column->getInt(i);
                if (is_north_value != 0 && is_north_value != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Argument is_north of function {} must be 0 or 1, but got {}", getName(), is_north_value);
                is_north = (is_north_value != 0);
            }

            utmToWGS84(easting, northing, static_cast<UInt8>(zone), is_north, longitude_data[i], latitude_data[i]);
        }

        return ColumnTuple::create(Columns{std::move(col_longitude), std::move(col_latitude)});
    }
};

}

REGISTER_FUNCTION(UTMToGeo)
{
    FunctionDocumentation::Description description = R"(
Converts [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) coordinates back to WGS84 geographic coordinates (longitude, latitude). This is the inverse of [`geoToUTM`](#geotoutm).
    )";
    FunctionDocumentation::Syntax syntax = "UTMToGeo(easting, northing, zone, is_north)";
    FunctionDocumentation::Arguments arguments = {
        {"easting", "Easting in metres (includes the 500000 m false easting).", {"(U)Int*", "Float*"}},
        {"northing", "Northing in metres (includes the 10000000 m false northing on the southern hemisphere).", {"(U)Int*", "Float*"}},
        {"zone", "UTM zone number. Range: `[1, 60]`.", {"(U)Int*"}},
        {"is_north", "Hemisphere indicator: `1` for northern hemisphere, `0` for southern. Also accepts an MGRS latitude band letter (`String`/`FixedString`) as returned by `geoToUTM`: bands `N`..`X` map to the northern hemisphere, `C`..`M` to the southern (`I` and `O` are excluded).", {"(U)Int*", "String", "FixedString"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a named tuple `(longitude, latitude)` in degrees.", {"Tuple(Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Integer is_north", "SELECT UTMToGeo(448251.6, 5411935.13, 31, 1)", "(2.2944970289079203,48.85822204127082)"},
        {"Band letter from geoToUTM", "WITH geoToUTM(2.294497, 48.858222) AS utm SELECT UTMToGeo(utm.easting, utm.northing, utm.zone, utm.band)", "(2.294497...,48.858222...)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionUTMToGeo>(documentation);
}

}
