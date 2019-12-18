#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include "registerFunctions.h"


namespace DB
{

using FunctionAddSeconds = FunctionDateOrDateTimeAddInterval<AddSecondsImpl>;

void registerFunctionAddSeconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddSeconds>();
}

}


