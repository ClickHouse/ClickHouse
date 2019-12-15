#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include "registerFunctions.h"


namespace DB
{

using FunctionAddQuarters = FunctionDateOrDateTimeAddInterval<AddQuartersImpl>;

void registerFunctionAddQuarters(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddQuarters>();
}

}


