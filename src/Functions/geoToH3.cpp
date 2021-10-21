#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_H3

#include <array>
#include <math.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Implements the function geoToH3 which takes 3 arguments (latitude, longitude and h3 resolution)
/// and returns h3 index of this point
class FunctionGeoToH3 : public IFunction
{
public:
    static constexpr auto name = "geoToH3";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoToH3>(); }

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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_lon = arguments[0].column.get();
        const auto * col_lat = arguments[1].column.get();
        const auto * col_res = arguments[2].column.get();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (const auto row : collections::range(0, input_rows_count))
        {
            const double lon = col_lon->getFloat64(row);
            const double lat = col_lat->getFloat64(row);
            const UInt8 res = col_res->getUInt(row);

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

void registerFunctionGeoToH3(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGeoToH3>();
}

}

#endif
