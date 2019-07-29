namespace DB
{

class FunctionFactory;

void registerFunctionSymbolizeAddress(FunctionFactory & factory);
void registerFunctionDemangle(FunctionFactory & factory);
void registerFunctionAddressToLine(FunctionFactory & factory);

void registerFunctionsIntrospection(FunctionFactory & factory)
{
    registerFunctionSymbolizeAddress(factory);
    registerFunctionDemangle(factory);
    registerFunctionAddressToLine(factory);
}

}

