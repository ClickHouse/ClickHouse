#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include "registerFunctions.h"


namespace DB
{

using FunctionAddHours = FunctionDateOrDateTimeAddInterval<AddHoursImpl>;

void registerFunctionAddHours(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddHours>();
}

}


