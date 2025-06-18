#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfSecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfSecondImpl>;

REGISTER_FUNCTION(ToStartOfSecond)
{
    factory.registerFunction<FunctionToStartOfSecond>();
}

}
