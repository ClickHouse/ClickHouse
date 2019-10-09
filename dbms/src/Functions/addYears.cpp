#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddYears = FunctionDateOrDateTimeAddInterval<AddOnDateTime64Mixin<AddYearsImpl>>;

void registerFunctionAddYears(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddYears>();
}

}


