namespace DB
{

class FunctionFactory;

void registerFunctionRand(FunctionFactory & factory);
void registerFunctionRand64(FunctionFactory & factory);
void registerFunctionRandConstant(FunctionFactory & factory);
void registerFunctionGenerateUUIDv4(FunctionFactory & factory);

void registerFunctionsRandom(FunctionFactory & factory)
{
    registerFunctionRand(factory);
    registerFunctionRand64(factory);
    registerFunctionRandConstant(factory);
    registerFunctionGenerateUUIDv4(factory);
}

}


