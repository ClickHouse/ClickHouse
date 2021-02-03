#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfDay = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfDayImpl>;

void registerFunctionToStartOfDay(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfDay>();
}

}


