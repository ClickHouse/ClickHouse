#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToTimeWithFixedDate = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeWithFixedDateImpl>;

REGISTER_FUNCTION(ToTimeWithFixedDate)
{
    factory.registerFunction<FunctionToTimeWithFixedDate>();
}

}


