namespace DB
{

class FunctionFactory;

#ifdef __ELF__
void registerFunctionAddressToSymbol(FunctionFactory & factory);
void registerFunctionAddressToLine(FunctionFactory & factory);
#endif
void registerFunctionDemangle(FunctionFactory & factory);
void registerFunctionTrap(FunctionFactory & factory);

void registerFunctionsIntrospection(FunctionFactory & factory)
{
#ifdef __ELF__
    registerFunctionAddressToSymbol(factory);
    registerFunctionAddressToLine(factory);
#endif
    registerFunctionDemangle(factory);
    registerFunctionTrap(factory);
}

}

