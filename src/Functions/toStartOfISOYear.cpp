#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfISOYear = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfISOYearImpl>;

REGISTER_FUNCTION(ToStartOfISOYear)
{
    factory.registerFunction<FunctionToStartOfISOYear>();
}

}


