#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfFiveMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFiveMinuteImpl>;

void registerFunctionToStartOfFiveMinute(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfFiveMinute>();
}

}


