#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractNanoseconds = FunctionDateOrDateTimeAddInterval<SubtractNanosecondsImpl>;
void registerFunctionSubtractNanoseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractNanoseconds>();
}

using FunctionSubtractMicroseconds = FunctionDateOrDateTimeAddInterval<SubtractMicrosecondsImpl>;
void registerFunctionSubtractMicroseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractMicroseconds>();
}

using FunctionSubtractMilliseconds = FunctionDateOrDateTimeAddInterval<SubtractMillisecondsImpl>;
void registerFunctionSubtractMilliseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractMilliseconds>();
}

}


