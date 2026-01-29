#include "config.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include "s2_fwd.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;

}

namespace
{

class FunctionS2RectContains : public IFunction
{
public:
    static constexpr auto name = "s2RectContains";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2RectContains>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const auto * arg = arguments[i].get();
            if (!WhichDataType(arg).isUInt64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be UInt64",
                    arg->getName(), i, getName());
        }

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_lo = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_lo)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_low = col_lo->getData();

        const auto * col_hi = checkAndGetColumn<ColumnUInt64>(non_const_arguments[1].column.get());
        if (!col_hi)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[1].type->getName(),
                2,
                getName());
        const auto & data_hi = col_hi->getData();

        const auto * col_point = checkAndGetColumn<ColumnUInt64>(non_const_arguments[2].column.get());
        if (!col_point)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[2].type->getName(),
                3,
                getName());
        const auto & data_point = col_point->getData();

        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto lo = S2CellId(data_low[row]);
            const auto hi = S2CellId(data_hi[row]);
            const auto point = S2CellId(data_point[row]);

            S2LatLngRect rect(lo.ToLatLng(), hi.ToLatLng());

            if (!point.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Point is invalid. For valid point the latitude is between -90 and 90 degrees inclusive "
                    "and the longitude is between -180 and 180 degrees inclusive.");

            if (!rect.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Rectangle is invalid. For valid rectangles the latitude bounds do not exceed "
                    "Pi/2 in absolute value and the longitude bounds do not exceed Pi in absolute value. "
                    "Also, if either the latitude or longitude bound is empty then both must be. ");

            dst_data.emplace_back(rect.Contains(point.ToLatLng()));
        }

        return dst;
    }

};

}

REGISTER_FUNCTION(S2RectContains)
{
    factory.registerFunction<FunctionS2RectContains>();
}


}

#endif
