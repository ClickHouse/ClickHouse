#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMicroseconds = FunctionDateOrDateTimeAddInterval<AddMicrosecondsImpl>;

REGISTER_FUNCTION(AddMicroseconds)
{
    factory.registerFunction<FunctionAddMicroseconds>();
}

}


