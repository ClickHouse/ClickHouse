#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddWeeks = FunctionDateOrDateTimeAddInterval<AddWeeksImpl>;

REGISTER_FUNCTION(AddWeeks)
{
    factory.registerFunction<FunctionAddWeeks>();
}

}


