#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfTenMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfTenMinutesImpl>;

void registerFunctionToStartOfTenMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfTenMinutes>();
}

}


