#include <Functions/IFunctionImpl.h>

#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddHours = FunctionDateOrDateTimeAddInterval<AddHoursImpl>;

void registerFunctionAddHours(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddHours>();
}

}


