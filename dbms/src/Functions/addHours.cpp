#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddHours = FunctionDateOrDateTimeAddInterval<AddOnDateTime64Mixin<AddHoursImpl>>;

void registerFunctionAddHours(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddHours>();
}

}


