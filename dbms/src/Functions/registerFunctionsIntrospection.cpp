namespace DB
{

class FunctionFactory;

void registerFunctionAddressToSymbol(FunctionFactory & factory);
void registerFunctionDemangle(FunctionFactory & factory);
void registerFunctionAddressToLine(FunctionFactory & factory);

void registerFunctionsIntrospection(FunctionFactory & factory)
{
    registerFunctionAddressToSymbol(factory);
    registerFunctionDemangle(factory);
    registerFunctionAddressToLine(factory);
}

}

