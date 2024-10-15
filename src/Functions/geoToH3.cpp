#include "config.h"

#if USE_H3

#include <array>
#include <cmath>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
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

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_lon = checkAndGetColumn<ColumnFloat64>(non_const_arguments[0].column.get());
        if (!col_lon)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be Float64.",
                    arguments[0].type->getName(),
                    1,
                    getName());
        const auto & data_lon = col_lon->getData();

        const auto * col_lat = checkAndGetColumn<ColumnFloat64>(non_const_arguments[1].column.get());
        if (!col_lat)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be Float64.",
                    arguments[1].type->getName(),
                    2,
                    getName());
        const auto & data_lat = col_lat->getData();

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
    factory.registerFunction<FunctionGeoToH3>();
}

}

#endif
