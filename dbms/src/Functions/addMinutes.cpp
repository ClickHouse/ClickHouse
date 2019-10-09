#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMinutes = FunctionDateOrDateTimeAddInterval<AddOnDateTime64Mixin<AddMinutesImpl>>;

void registerFunctionAddMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMinutes>();
}

}


