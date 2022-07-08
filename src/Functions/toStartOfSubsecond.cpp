#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMillisecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMillisecondImpl>;

void registerFunctionToStartOfMillisecond(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfMillisecond>();
}

using FunctionToStartOfMicrosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMicrosecondImpl>;

void registerFunctionToStartOfMicrosecond(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfMicrosecond>();
}

using FunctionToStartOfNanosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfNanosecondImpl>;

void registerFunctionToStartOfNanosecond(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfNanosecond>();
}

}
