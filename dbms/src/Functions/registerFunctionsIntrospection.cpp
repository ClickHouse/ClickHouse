#include <Core/Defines.h>

namespace DB
{

class FunctionFactory;

void registerFunctionAddressToSymbol(FunctionFactory & factory);
void registerFunctionDemangle(FunctionFactory & factory);
void registerFunctionAddressToLine(FunctionFactory & factory);
void registerFunctionTrap(FunctionFactory & factory);

void registerFunctionsIntrospection(FunctionFactory & factory)
{
#if defined (OS_LINUX)
    registerFunctionAddressToSymbol(factory);
    registerFunctionDemangle(factory);
    registerFunctionAddressToLine(factory);
    registerFunctionTrap(factory);
#else
    UNUSED(factory);
#endif
}

}

