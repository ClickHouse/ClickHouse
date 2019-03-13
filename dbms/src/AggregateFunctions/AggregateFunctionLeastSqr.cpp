#include <AggregateFunctions/AggregateFunctionLeastSqr.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionLeastSqr(
    const String & name,
    const DataTypes & arguments,
    const Array & params
)
{
    assertNoParameters(name, params);
    assertBinary(name, arguments);

    const IDataType * x_arg = arguments.front().get();

    WhichDataType which_x {
        x_arg
    };

    if (
        !which_x.isNativeUInt()
        && !which_x.isNativeInt()
        && !which_x.isFloat()
    )
        throw Exception {
            "Illegal type " + x_arg->getName()
                + " of first argument of aggregate function "
                + name + ", must be Native Int, Native UInt or Float",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
        };

    const IDataType * y_arg = arguments.back().get();

    WhichDataType which_y {
        y_arg
    };

    if (
        !which_y.isNativeUInt()
        && !which_y.isNativeInt()
        && !which_y.isFloat()
    )
        throw Exception {
            "Illegal type " + y_arg->getName()
                + " of second argument of aggregate function "
                + name + ", must be Native Int, Native UInt or Float",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
        };

    if (which_x.isNativeUInt() && which_y.isNativeUInt())
        return std::make_shared<AggregateFunctionLeastSqr<UInt64, UInt64>>(
            arguments,
            params
        );
    else if (which_x.isNativeUInt() && which_y.isNativeInt())
        return std::make_shared<AggregateFunctionLeastSqr<UInt64, Int64>>(
            arguments,
            params
        );
    else if (which_x.isNativeUInt() && which_y.isFloat())
        return std::make_shared<AggregateFunctionLeastSqr<UInt64, Float64>>(
            arguments,
            params
        );
    else if (which_x.isNativeInt() && which_y.isNativeUInt())
        return std::make_shared<AggregateFunctionLeastSqr<Int64, UInt64>>(
            arguments,
            params
        );
    else if (which_x.isNativeInt() && which_y.isNativeInt())
        return std::make_shared<AggregateFunctionLeastSqr<Int64, Int64>>(
            arguments,
            params
        );
    else if (which_x.isNativeInt() && which_y.isFloat())
        return std::make_shared<AggregateFunctionLeastSqr<Int64, Float64>>(
            arguments,
            params
        );
    else if (which_x.isFloat() && which_y.isNativeUInt())
        return std::make_shared<AggregateFunctionLeastSqr<Float64, UInt64>>(
            arguments,
            params
        );
    else if (which_x.isFloat() && which_y.isNativeInt())
        return std::make_shared<AggregateFunctionLeastSqr<Float64, Int64>>(
            arguments,
            params
        );
    else // if (which_x.isFloat() && which_y.isFloat())
        return std::make_shared<AggregateFunctionLeastSqr<Float64, Float64>>(
            arguments,
            params
        );
}

}

void registerAggregateFunctionLeastSqr(AggregateFunctionFactory & factory)
{
    factory.registerFunction("leastSqr", createAggregateFunctionLeastSqr);
}

}
