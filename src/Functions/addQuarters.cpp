#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddQuarters = FunctionDateOrDateTimeAddInterval<AddQuartersImpl>;

REGISTER_FUNCTION(AddQuarters)
{
    factory.registerFunction<FunctionAddQuarters>();
}

}


