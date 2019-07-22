#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractQuarters = FunctionDateOrDateTimeAddInterval<SubtractQuartersImpl>;

void registerFunctionSubtractQuarters(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractQuarters>();
}

}


