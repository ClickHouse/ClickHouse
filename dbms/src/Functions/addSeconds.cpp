#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddSeconds = FunctionDateOrDateTimeAddInterval<AddOnDateTime64Mixin<AddSecondsImpl>>;

void registerFunctionAddSeconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddSeconds>();
}

}


