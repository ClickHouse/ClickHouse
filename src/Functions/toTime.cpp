#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeImpl>;

void registerFunctionToTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTime>();
}

}


