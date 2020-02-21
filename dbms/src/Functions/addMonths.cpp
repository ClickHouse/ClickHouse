#include <Functions/IFunctionImpl.h>

#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddMonthsImpl>;

void registerFunctionAddMonths(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMonths>();
}

}


