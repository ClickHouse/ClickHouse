#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UTMCoordinates.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// MGRSToGeo(mgrs) => (longitude Float64, latitude Float64)
class FunctionMGRSToGeo final : public IFunction
{
public:
    static constexpr auto name = "MGRSToGeo";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMGRSToGeo>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"mgrs", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
            Strings{"longitude", "latitude"});
    }

    template <typename ColumnType>
    bool tryExecute(const IColumn * mgrs_column, ColumnPtr & result_column, size_t input_rows_count) const
    {
        const auto * mgrs = checkAndGetColumn<ColumnType>(mgrs_column);
        if (!mgrs)
            return false;

        auto col_longitude = ColumnFloat64::create(input_rows_count);
        auto col_latitude = ColumnFloat64::create(input_rows_count);

        auto & longitude_data = col_longitude->getData();
        auto & latitude_data = col_latitude->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view value = mgrs->getDataAt(i);
            if constexpr (std::is_same_v<ColumnType, ColumnFixedString>)
            {
                /// FixedString null-pads values shorter than N; drop the trailing padding before decoding.
                size_t length = value.size();
                while (length > 0 && value[length - 1] == '\0')
                    --length;
                value = value.substr(0, length);
            }

            const MGRSCoordinate coordinate = mgrsDecode(value);
            longitude_data[i] = coordinate.longitude;
            latitude_data[i] = coordinate.latitude;
        }

        result_column = ColumnTuple::create(Columns{std::move(col_longitude), std::move(col_latitude)});
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * mgrs = arguments[0].column.get();
        ColumnPtr result_column;

        if (tryExecute<ColumnString>(mgrs, result_column, input_rows_count)
            || tryExecute<ColumnFixedString>(mgrs, result_column, input_rows_count))
            return result_column;

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unsupported argument type {} of function {}",
            arguments[0].column->getName(), getName());
    }
};

}

REGISTER_FUNCTION(MGRSToGeo)
{
    FunctionDocumentation::Description description = R"(
Decodes a [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) string into WGS84 geographic coordinates (longitude, latitude). This is the inverse of [`geoToMGRS`](#geotomgrs).

The returned point is the centre of the referenced grid square, so the precision of the result matches the precision encoded in the string. Whitespace in the input is ignored and letters are case-insensitive.
    )";
    FunctionDocumentation::Syntax syntax = "MGRSToGeo(mgrs)";
    FunctionDocumentation::Arguments arguments = {
        {"mgrs", "MGRS reference string to decode.", {"String", "FixedString"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a named tuple `(longitude, latitude)` in degrees.", {"Tuple(Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT MGRSToGeo('31UDQ4825111935')", "(2.294495618908297,48.85822536113692)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMGRSToGeo>(documentation);
}

}
