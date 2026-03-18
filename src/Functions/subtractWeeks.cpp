#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractWeeks = FunctionDateOrDateTimeAddInterval<SubtractWeeksImpl>;

REGISTER_FUNCTION(SubtractWeeks)
{
    factory.registerFunction<FunctionSubtractWeeks>();
}

}


