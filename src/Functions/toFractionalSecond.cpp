#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>

namespace DB
{
using FunctionToFractionalSecond = FunctionDateOrDateTimeToSomething<DataTypeInt64, ToFractionalSecondImpl>;

REGISTER_FUNCTION(ToFractionalSecond)
{
    factory.registerFunction<FunctionToFractionalSecond>();
}
}
