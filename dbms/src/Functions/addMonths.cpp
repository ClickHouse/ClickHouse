#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddOnDateTime64Mixin<AddMonthsImpl>>;

void registerFunctionAddMonths(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMonths>();
}

}


