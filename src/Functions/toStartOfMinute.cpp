#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfMinuteImpl>;

void registerFunctionToStartOfMinute(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfMinute>();
}

}


