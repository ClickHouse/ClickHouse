namespace DB
{

class FunctionFactory;

void registerFunctionsReinterpretAs(FunctionFactory & factory);

void registerFunctionsReinterpret(FunctionFactory & factory)
{
    registerFunctionsReinterpretAs(factory);
}

}
