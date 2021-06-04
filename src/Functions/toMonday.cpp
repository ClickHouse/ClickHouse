#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToMonday = FunctionDateOrDateTimeToSomething<DataTypeDate, ToMondayImpl>;

void registerFunctionToMonday(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToMonday>();
}

}


