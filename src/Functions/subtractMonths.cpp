#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMonths = FunctionDateOrDateTimeAddInterval<SubtractMonthsImpl>;

REGISTER_FUNCTION(SubtractMonths)
{
    factory.registerFunction<FunctionSubtractMonths>();
}

}


