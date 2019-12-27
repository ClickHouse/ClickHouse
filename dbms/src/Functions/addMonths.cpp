#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include "registerFunctions.h"


namespace DB
{

using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddMonthsImpl>;

void registerFunctionAddMonths(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMonths>();
}

}


