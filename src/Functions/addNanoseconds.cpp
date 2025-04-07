#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddNanoseconds = FunctionDateOrDateTimeAddInterval<AddNanosecondsImpl>;

REGISTER_FUNCTION(AddNanoseconds)
{
    factory.registerFunction<FunctionAddNanoseconds>();
}

}


