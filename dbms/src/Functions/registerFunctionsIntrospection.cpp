#include "registerFunctions.h"

namespace DB
{
void registerFunctionsIntrospection(FunctionFactory & factory)
{
#if defined(OS_LINUX)
    registerFunctionAddressToSymbol(factory);
    registerFunctionAddressToLine(factory);
#endif
    registerFunctionDemangle(factory);
    registerFunctionTrap(factory);
}

}
