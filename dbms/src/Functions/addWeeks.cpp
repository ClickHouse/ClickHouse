#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddWeeks = FunctionDateOrDateTimeAddInterval<AddOnDateTime64Mixin<AddWeeksImpl>>;

void registerFunctionAddWeeks(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddWeeks>();
}

}


