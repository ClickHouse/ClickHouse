#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfFiveMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFiveMinutesImpl>;

void registerFunctionToStartOfFiveMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfFiveMinutes>();
    factory.registerAlias("toStartOfFiveMinute", FunctionToStartOfFiveMinutes::name);
}

}


