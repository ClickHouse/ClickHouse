#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractSeconds = FunctionDateOrDateTimeAddInterval<SubtractSecondsImpl>;

REGISTER_FUNCTION(SubtractSeconds)
{
    factory.registerFunction<FunctionSubtractSeconds>();
}

}


