#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMilliseconds = FunctionDateOrDateTimeAddInterval<SubtractMillisecondsImpl>;
REGISTER_FUNCTION(SubtractMilliseconds)
{
    factory.registerFunction<FunctionSubtractMilliseconds>();
}

}


