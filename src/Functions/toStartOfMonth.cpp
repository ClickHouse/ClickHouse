#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToStartOfMonthImpl>;

REGISTER_FUNCTION(ToStartOfMonth)
{
    factory.registerFunction<FunctionToStartOfMonth>();
}

}


