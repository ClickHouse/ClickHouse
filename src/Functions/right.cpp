#include <Functions/FunctionFactory.h>
#include <Functions/LeftRight.h>

namespace DB
{

REGISTER_FUNCTION(Right)
{
    factory.registerFunction<FunctionLeftRight<false, SubstringDirection::Right>>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionLeftRight<true, SubstringDirection::Right>>({}, FunctionFactory::Case::Sensitive);
}

}
