#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractSeconds = FunctionDateOrDateTimeAddInterval<SubtractSecondsImpl>;

void registerFunctionSubtractSeconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractSeconds>();
}

}


