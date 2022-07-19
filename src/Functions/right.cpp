#include <Functions/FunctionFactory.h>
#include <Functions/LeftRight.h>

namespace DB
{

void registerFunctionRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeftRight<false, SubstringDirection::Right>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionLeftRight<true, SubstringDirection::Right>>(FunctionFactory::CaseSensitive);
}

}
