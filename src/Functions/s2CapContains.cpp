#include "config_functions.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>
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

/**
 * The cap represents a portion of the sphere that has been cut off by a plane.
 * It is defined by a point on a sphere and a radius in degrees.
 * Imagine that we draw a line through the center of the sphere and our point.
 * An infinite number of planes pass through this line, but any plane will intersect the cap in two points.
 * Thus the angle is defined by one of this points and the entire line.
 * So, the radius of Pi/2 defines a hemisphere and the radius of Pi defines a whole sphere.
 *
 * This function returns whether a cap contains a point.
 */
class FunctionS2CapContains : public IFunction
{
public:
    static constexpr auto name = "s2CapContains";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2CapContains>();
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
        for (size_t index = 0; index < getNumberOfArguments(); ++index)
        {
            const auto * arg = arguments[index].get();

            /// Radius
            if (index == 1)
            {
                if (!WhichDataType(arg).isFloat64())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function {}. Must be Float64",
                        arg->getName(), 2, getName());
            }
            else if (!WhichDataType(arg).isUInt64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be UInt64",
                    arg->getName(), index + 1, getName());
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_center = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_center)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_center = col_center->getData();

        const auto * col_degrees = checkAndGetColumn<ColumnFloat64>(non_const_arguments[1].column.get());
        if (!col_degrees)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arguments[1].type->getName(),
                2,
                getName());
        const auto & data_degrees = col_degrees->getData();


        const auto * col_point = checkAndGetColumn<ColumnUInt64>(non_const_arguments[2].column.get());
        if (!col_point)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[2].type->getName(),
                3,
                getName());
        const auto & data_point = col_point->getData();


        auto dst = ColumnUInt8::create();
        auto & dst_data = dst->getData();
        dst_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto center = S2CellId(data_center[row]);
            const Float64 degrees = data_degrees[row];
            const auto point = S2CellId(data_point[row]);

            if (isNaN(degrees))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Radius of the cap must not be nan");

            if (std::isinf(degrees))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Radius of the cap must not be infinite");

            if (!center.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Center is not valid");

            if (!point.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Point is not valid");

            S1Angle angle = S1Angle::Degrees(degrees);
            S2Cap cap(center.ToPoint(), angle);

            dst_data.emplace_back(cap.Contains(point.ToPoint()));
        }

        return dst;
    }
};

}

void registerFunctionS2CapContains(FunctionFactory & factory)
{
    factory.registerFunction<FunctionS2CapContains>();
}


}

#endif
