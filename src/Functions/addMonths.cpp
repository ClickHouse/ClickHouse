#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddMonthsImpl>;

REGISTER_FUNCTION(AddMonths)
{
    factory.registerFunction<FunctionAddMonths>();
}

}


