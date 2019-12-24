#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractYears = FunctionDateOrDateTimeAddInterval<SubtractYearsImpl>;

void registerFunctionSubtractYears(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractYears>();
}

}


