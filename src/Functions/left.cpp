#include <Functions/FunctionFactory.h>
#include <Functions/LeftRight.h>

namespace DB
{

void registerFunctionLeft(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeftRight<false, SubstringDirection::Left>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionLeftRight<true, SubstringDirection::Left>>(FunctionFactory::CaseSensitive);
}

}
