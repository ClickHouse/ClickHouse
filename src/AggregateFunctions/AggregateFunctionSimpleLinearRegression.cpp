#include <AggregateFunctions/AggregateFunctionSimpleLinearRegression.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <Core/TypeListNumber.h>
#include "registerAggregateFunctions.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionSimpleLinearRegression(
    const String & name,
    const DataTypes & arguments,
    const Array & params
)
{
    assertNoParameters(name, params);
    assertBinary(name, arguments);

    const IDataType * x_arg = arguments.front().get();
    WhichDataType which_x = x_arg;

    const IDataType * y_arg = arguments.back().get();
    WhichDataType which_y = y_arg;


    #define FOR_LEASTSQR_TYPES_2(M, T) \
        M(T, UInt8) \
        M(T, UInt16) \
        M(T, UInt32) \
        M(T, UInt64) \
        M(T, Int8) \
        M(T, Int16) \
        M(T, Int32) \
        M(T, Int64) \
        M(T, Float32) \
        M(T, Float64)
    #define FOR_LEASTSQR_TYPES(M) \
        FOR_LEASTSQR_TYPES_2(M, UInt8) \
        FOR_LEASTSQR_TYPES_2(M, UInt16) \
        FOR_LEASTSQR_TYPES_2(M, UInt32) \
        FOR_LEASTSQR_TYPES_2(M, UInt64) \
        FOR_LEASTSQR_TYPES_2(M, Int8) \
        FOR_LEASTSQR_TYPES_2(M, Int16) \
        FOR_LEASTSQR_TYPES_2(M, Int32) \
        FOR_LEASTSQR_TYPES_2(M, Int64) \
        FOR_LEASTSQR_TYPES_2(M, Float32) \
        FOR_LEASTSQR_TYPES_2(M, Float64)
    #define DISPATCH(T1, T2) \
        if (which_x.idx == TypeIndex::T1 && which_y.idx == TypeIndex::T2) \
            return std::make_shared<AggregateFunctionSimpleLinearRegression<T1, T2>>(/* NOLINT */ \
                arguments, \
                params \
            );

    FOR_LEASTSQR_TYPES(DISPATCH)

    #undef FOR_LEASTSQR_TYPES_2
    #undef FOR_LEASTSQR_TYPES
    #undef DISPATCH

    throw Exception(
        "Illegal types ("
            + x_arg->getName() + ", " + y_arg->getName()
            + ") of arguments of aggregate function " + name
            + ", must be Native Ints, Native UInts or Floats",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
    );
}

}

void registerAggregateFunctionSimpleLinearRegression(AggregateFunctionFactory & factory)
{
    factory.registerFunction("simpleLinearRegression", createAggregateFunctionSimpleLinearRegression);
}

}
