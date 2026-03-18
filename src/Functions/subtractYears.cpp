#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractYears = FunctionDateOrDateTimeAddInterval<SubtractYearsImpl>;

REGISTER_FUNCTION(SubtractYears)
{
    factory.registerFunction<FunctionSubtractYears>();
}

}


