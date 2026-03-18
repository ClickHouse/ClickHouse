#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddYears = FunctionDateOrDateTimeAddInterval<AddYearsImpl>;

REGISTER_FUNCTION(AddYears)
{
    factory.registerFunction<FunctionAddYears>();
}

}


