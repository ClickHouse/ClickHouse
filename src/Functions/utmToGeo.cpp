#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UTMCoordinates.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
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
            {"is_north", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "(U)Int* (0 or 1)"}};
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

            const Int64 is_north_value = is_north_column->getInt(i);
            if (is_north_value != 0 && is_north_value != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Argument is_north of function {} must be 0 or 1, but got {}", getName(), is_north_value);
            const bool is_north = is_north_value != 0;

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
        {"is_north", "Hemisphere: `1` for the northern hemisphere, `0` for the southern.", {"(U)Int*"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a named tuple `(longitude, latitude)` in degrees.", {"Tuple(Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT UTMToGeo(448251.6, 5411935.13, 31, 1)", "(2.2944970289079203,48.85822204127082)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionUTMToGeo>(documentation);
}

}
