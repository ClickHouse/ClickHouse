namespace DB
{

class FunctionFactory;

void registerFunctionAddressToSymbol(FunctionFactory & factory);
void registerFunctionDemangle(FunctionFactory & factory);
void registerFunctionAddressToLine(FunctionFactory & factory);
void registerFunctionTrap(FunctionFactory & factory);

void registerFunctionsIntrospection(FunctionFactory & factory)
{
    registerFunctionAddressToSymbol(factory);
    registerFunctionDemangle(factory);
    registerFunctionAddressToLine(factory);
    registerFunctionTrap(factory);
}

}

