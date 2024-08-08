#include <Functions/FunctionFactory.h>
#include <Functions/LeftRight.h>

namespace DB
{

REGISTER_FUNCTION(Left)
{
    factory.registerFunction<FunctionLeftRight<false, SubstringDirection::Left>>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionLeftRight<true, SubstringDirection::Left>>({}, FunctionFactory::Case::Sensitive);
}

}
