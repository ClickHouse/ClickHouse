#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddDaysImpl>;

void registerFunctionAddDays(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddDays>();
}

}


