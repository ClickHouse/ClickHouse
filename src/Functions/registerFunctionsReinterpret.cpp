namespace DB
{

class FunctionFactory;

void registerFunctionsReinterpretAs(FunctionFactory & factory);
void registerFunctionReinterpretAsString(FunctionFactory & factory);
void registerFunctionReinterpretAsFixedString(FunctionFactory & factory);

void registerFunctionsReinterpret(FunctionFactory & factory)
{
    registerFunctionsReinterpretAs(factory);
    registerFunctionReinterpretAsString(factory);
    registerFunctionReinterpretAsFixedString(factory);
}

}
