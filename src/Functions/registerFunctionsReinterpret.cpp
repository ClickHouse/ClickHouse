namespace DB
{

class FunctionFactory;

void registerFunctionsReinterpretStringAs(FunctionFactory & factory);
void registerFunctionReinterpretAsString(FunctionFactory & factory);
void registerFunctionReinterpretAsFixedString(FunctionFactory & factory);

void registerFunctionsReinterpret(FunctionFactory & factory)
{
    registerFunctionsReinterpretStringAs(factory);
    registerFunctionReinterpretAsString(factory);
    registerFunctionReinterpretAsFixedString(factory);
}

}
