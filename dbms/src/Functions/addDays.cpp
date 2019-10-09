#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddOnDateTime64Mixin<AddDaysImpl>>;

void registerFunctionAddDays(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddDays>();
}

}


