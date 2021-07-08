#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>
#include <common/range.h>

#include "s2_fwd.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
            else if (!WhichDataType(arg).isUInt64()) {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be UInt64",
                    arg->getName(), index + 1, getName());
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_center = arguments[0].column.get();
        const auto * col_degrees = arguments[1].column.get();
        const auto * col_point = arguments[2].column.get();

        auto dst = ColumnUInt8::create();
        auto & dst_data = dst->getData();
        dst_data.reserve(input_rows_count);

        for (const auto row : collections::range(0, input_rows_count))
        {
            const UInt64 center = col_center->getUInt(row);
            const Float64 degrees = col_degrees->getFloat64(row);
            const UInt64 point = col_point->getUInt(row);

            S1Angle angle = S1Angle::Degrees(degrees);
            S2Cap cap(S2CellId(center).ToPoint(), angle);

            dst_data.emplace_back(cap.Contains(S2CellId(point).ToPoint()));
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
