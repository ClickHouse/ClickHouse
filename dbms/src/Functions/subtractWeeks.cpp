#include <Functions/IFunctionImpl.h>

#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractWeeks = FunctionDateOrDateTimeAddInterval<SubtractWeeksImpl>;

void registerFunctionSubtractWeeks(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractWeeks>();
}

}


