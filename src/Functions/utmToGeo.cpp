#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UTMCoordinates.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
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

    /// The default Nullable wrapper would execute the function on the values that Nullable columns hold at NULL
    /// rows (typically defaults such as the empty string), and the band validation would reject them before the
    /// null map is re-applied. Handle Nullable arguments explicitly instead: NULL rows are skipped, not validated.
    bool useDefaultImplementationForNulls() const override { return false; }

    /// The Dynamic and Variant adaptors default to useDefaultImplementationForNulls, but disabling them here is
    /// unnecessary: the adaptors dispatch on the concrete types held inside the column and handle NULL rows
    /// themselves, and this function accepts Nullable arguments anyway. Keep them enabled so that Dynamic and
    /// Variant arguments still dispatch to the matching alternative.
    bool useDefaultImplementationForDynamic() const override { return true; }
    bool useDefaultImplementationForVariant() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        bool has_nullable = false;
        ColumnsWithTypeAndName nested_arguments = arguments;
        for (auto & argument : nested_arguments)
        {
            if (argument.type && argument.type->onlyNull())
                return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
            if (argument.type && argument.type->isNullable())
            {
                has_nullable = true;
                argument.type = removeNullable(argument.type);
                argument.column = nullptr;
            }
        }

        FunctionArgumentDescriptors mandatory_args{
            {"easting", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {"northing", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {"zone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "(U)Int*"},
            {"is_north", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isHemisphereArgument), nullptr,
                "(U)Int* (0 or 1) or String/FixedString (MGRS band letter)"}};
        validateFunctionArguments(*this, nested_arguments, mandatory_args);

        DataTypePtr result = std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
            Strings{"longitude", "latitude"});
        return has_nullable ? makeNullable(result) : result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// A NULL literal among the arguments makes the whole result NULL.
        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        /// Unwrap Nullable arguments and combine their null maps; NULL rows produce NULL and are not validated.
        const IColumn * columns[4];
        ColumnUInt8::MutablePtr result_null_map;
        ColumnPtr materialized[4];
        for (size_t arg = 0; arg < 4; ++arg)
        {
            materialized[arg] = arguments[arg].column->convertToFullColumnIfConst();
            columns[arg] = materialized[arg].get();
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(columns[arg]))
            {
                columns[arg] = &nullable->getNestedColumn();
                if (!result_null_map)
                    result_null_map = ColumnUInt8::create(input_rows_count, UInt8(0));
                auto & result_null_map_data = result_null_map->getData();
                const auto & null_map = nullable->getNullMapData();
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_null_map_data[i] |= null_map[i];
            }
        }

        const IColumn * easting_column = columns[0];
        const IColumn * northing_column = columns[1];
        const IColumn * zone_column = columns[2];
        const IColumn * hemisphere_column = columns[3];

        /// The hemisphere is given either as an integer flag or as the MGRS latitude band letter returned by `geoToUTM`.
        const bool hemisphere_is_band = isStringOrFixedString(*removeNullable(arguments[3].type));

        auto col_longitude = ColumnFloat64::create(input_rows_count);
        auto col_latitude = ColumnFloat64::create(input_rows_count);

        auto & longitude_data = col_longitude->getData();
        auto & latitude_data = col_latitude->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (result_null_map && result_null_map->getData()[i])
            {
                longitude_data[i] = 0.0;
                latitude_data[i] = 0.0;
                continue;
            }

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

        ColumnPtr result = ColumnTuple::create(Columns{std::move(col_longitude), std::move(col_latitude)});
        if (result_type->isNullable())
        {
            if (!result_null_map)
                result_null_map = ColumnUInt8::create(input_rows_count, UInt8(0));
            return ColumnNullable::create(result, std::move(result_null_map));
        }
        return result;
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
