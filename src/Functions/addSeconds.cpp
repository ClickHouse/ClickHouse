#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddSeconds = FunctionDateOrDateTimeAddInterval<AddSecondsImpl>;

REGISTER_FUNCTION(AddSeconds)
{
    factory.registerFunction<FunctionAddSeconds>();
}

}


