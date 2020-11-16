#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddWeeks = FunctionDateOrDateTimeAddInterval<AddWeeksImpl>;

void registerFunctionAddWeeks(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddWeeks>();
}

}


