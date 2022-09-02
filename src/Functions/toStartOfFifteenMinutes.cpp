#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfFifteenMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFifteenMinutesImpl>;

void registerFunctionToStartOfFifteenMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfFifteenMinutes>();
}

}


