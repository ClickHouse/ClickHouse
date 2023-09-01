#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfYear = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfYearImpl>;

REGISTER_FUNCTION(ToStartOfYear)
{
    factory.registerFunction<FunctionToStartOfYear>();
}

}


