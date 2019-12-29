#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include "registerFunctions.h"


namespace DB
{

using FunctionAddMinutes = FunctionDateOrDateTimeAddInterval<AddMinutesImpl>;

void registerFunctionAddMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMinutes>();
}

}


