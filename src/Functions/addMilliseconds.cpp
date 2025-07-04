#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMilliseconds = FunctionDateOrDateTimeAddInterval<AddMillisecondsImpl>;

REGISTER_FUNCTION(AddMilliseconds)
{
    factory.registerFunction<FunctionAddMilliseconds>();
}

}


