#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include "registerFunctions.h"


namespace DB
{

using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddDaysImpl>;

void registerFunctionAddDays(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddDays>();
}

}


