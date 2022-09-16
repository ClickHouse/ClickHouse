#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfMonthImpl>;

REGISTER_FUNCTION(ToStartOfMonth)
{
    factory.registerFunction<FunctionToStartOfMonth>();
}

}


