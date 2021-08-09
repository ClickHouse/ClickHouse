#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractWeeks = FunctionDateOrDateTimeAddInterval<SubtractWeeksImpl>;

void registerFunctionSubtractWeeks(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractWeeks>();
}

}


