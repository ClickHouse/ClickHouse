#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddHours = FunctionDateOrDateTimeAddInterval<AddHoursImpl>;

REGISTER_FUNCTION(AddHours)
{
    factory.registerFunction<FunctionAddHours>();
}

}


