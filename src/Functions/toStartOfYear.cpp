#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfYear = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfYearImpl>;

void registerFunctionToStartOfYear(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfYear>();
}

}


