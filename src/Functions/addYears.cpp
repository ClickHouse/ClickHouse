#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddYears = FunctionDateOrDateTimeAddInterval<AddYearsImpl>;

void registerFunctionAddYears(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddYears>();
}

}


