#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfISOYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfISOYearImpl>;

REGISTER_FUNCTION(ToStartOfISOYear)
{
    factory.registerFunction<FunctionToStartOfISOYear>();
}

}


