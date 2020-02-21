#include <Functions/IFunctionImpl.h>

#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddYears = FunctionDateOrDateTimeAddInterval<AddYearsImpl>;

void registerFunctionAddYears(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddYears>();
}

}


