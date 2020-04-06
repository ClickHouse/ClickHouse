#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractDays = FunctionDateOrDateTimeAddInterval<SubtractDaysImpl>;

void registerFunctionSubtractDays(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractDays>();
}

}


