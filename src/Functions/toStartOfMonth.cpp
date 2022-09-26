#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfMonthImpl>;

void registerFunctionToStartOfMonth(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfMonth>();
}

}


