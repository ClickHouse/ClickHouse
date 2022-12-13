#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMillisecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMillisecondImpl>;

REGISTER_FUNCTION(ToStartOfMillisecond)
{
    factory.registerFunction<FunctionToStartOfMillisecond>();
}

using FunctionToStartOfMicrosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMicrosecondImpl>;

REGISTER_FUNCTION(ToStartOfMicrosecond)
{
    factory.registerFunction<FunctionToStartOfMicrosecond>();
}

using FunctionToStartOfNanosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfNanosecondImpl>;

REGISTER_FUNCTION(ToStartOfNanosecond)
{
    factory.registerFunction<FunctionToStartOfNanosecond>();
}

}
