#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeTime, ToTimeImpl>;

REGISTER_FUNCTION(ToTime)
{
    factory.registerFunction<FunctionToTime>();
}

}


