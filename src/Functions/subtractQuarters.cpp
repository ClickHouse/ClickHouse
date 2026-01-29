#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractQuarters = FunctionDateOrDateTimeAddInterval<SubtractQuartersImpl>;

REGISTER_FUNCTION(SubtractQuarters)
{
    factory.registerFunction<FunctionSubtractQuarters>();
}

}


