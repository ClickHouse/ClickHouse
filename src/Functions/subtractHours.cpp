#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractHours = FunctionDateOrDateTimeAddInterval<SubtractHoursImpl>;

void registerFunctionSubtractHours(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractHours>();
}

}


