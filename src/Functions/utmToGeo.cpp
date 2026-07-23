#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UTMCoordinates.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/NaNUtils.h>

#include <cctype>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// The hemisphere argument accepts either an integer flag (0 or 1) or the MGRS latitude band letter that
/// `geoToUTM` returns, so that a `geoToUTM` result round-trips through `UTMToGeo` without a manual conversion.
bool isHemisphereArgument(const IDataType & type)
{
    return isInteger(type) || isStringOrFixedString(type);
}

/// Derives the hemisphere from an MGRS latitude band letter ('C'..'X', excluding 'I' and 'O', case-insensitive).
/// Bands 'C'..'M' lie on the southern hemisphere and 'N'..'X' on the northern one.
bool hemisphereFromBand(std::string_view band, const String & function_name)
{
    /// FixedString null-pads values shorter than its length; drop the trailing padding before validating.
    while (!band.empty() && band.back() == '\0')
        band.remove_suffix(1);

    const char letter = band.size() == 1 ? static_cast<char>(std::toupper(static_cast<unsigned char>(band[0]))) : 0;
    if (letter < 'C' || letter > 'X' || letter == 'I' || letter == 'O')
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The band argument of function {} must be a single MGRS latitude band letter in 'C'..'X' "
            "(excluding 'I' and 'O', case-insensitive), but got '{}'",
            function_name, String(band));

    return letter >= 'N';
}

/// UTMToGeo(easting, northing, zone, is_north) => (longitude Float64, latitude Float64)
/// The fourth argument is either the hemisphere flag (0 or 1) or the MGRS latitude band letter returned by `geoToUTM`.
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
            {"is_north", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isHemisphereArgument), nullptr,
                "(U)Int* (0 or 1) or String/FixedString (MGRS band letter)"}};
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
        const IColumn * hemisphere_column = arguments[3].column.get();

        /// The hemisphere is given either as an integer flag or as the MGRS latitude band letter returned by `geoToUTM`.
        const bool hemisphere_is_band = isStringOrFixedString(*arguments[3].type);

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

            bool is_north = false;
            if (hemisphere_is_band)
            {
                const std::string_view band = hemisphere_column->getDataAt(i);
                is_north = hemisphereFromBand(band, getName());
            }
            else
            {
                const Int64 is_north_value = hemisphere_column->getInt(i);
                if (is_north_value != 0 && is_north_value != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Argument is_north of function {} must be 0 or 1, but got {}", getName(), is_north_value);
                is_north = is_north_value != 0;
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

The fourth argument selects the hemisphere. It can be given either as an integer flag (`1` for the northern hemisphere, `0` for the southern) or as the MGRS latitude band letter that [`geoToUTM`](#geotoutm) returns, so a `geoToUTM` result round-trips through `UTMToGeo` directly.
    )";
    FunctionDocumentation::Syntax syntax = "UTMToGeo(easting, northing, zone, is_north)";
    FunctionDocumentation::Arguments arguments = {
        {"easting", "Easting in metres (includes the 500000 m false easting).", {"(U)Int*", "Float*"}},
        {"northing", "Northing in metres (includes the 10000000 m false northing on the southern hemisphere).", {"(U)Int*", "Float*"}},
        {"zone", "UTM zone number. Range: `[1, 60]`.", {"(U)Int*"}},
        {"is_north", "Hemisphere. Either an integer flag (`1` for the northern hemisphere, `0` for the southern) or the MGRS latitude band letter returned by `geoToUTM` (`'C'..'X'` excluding `'I'` and `'O'`, case-insensitive; band `>= 'N'` is the northern hemisphere).", {"(U)Int*", "String", "FixedString"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a named tuple `(longitude, latitude)` in degrees.", {"Tuple(Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT UTMToGeo(448251.6, 5411935.13, 31, 1)", "(2.2944970289079203,48.85822204127082)"},
        {"Round trip using the band letter from geoToUTM",
         "WITH geoToUTM(4.89, 52.36) AS utm SELECT UTMToGeo(utm.easting, utm.northing, utm.zone, utm.band)",
         "(4.890000000320752,52.36000000243152)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionUTMToGeo>(documentation);
}

}
