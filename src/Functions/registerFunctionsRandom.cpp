namespace DB
{
class FunctionFactory;

void registerFunctionRand(FunctionFactory & factory);
void registerFunctionRand64(FunctionFactory & factory);
void registerFunctionRandConstant(FunctionFactory & factory);
void registerFunctionGenerateUUIDv4(FunctionFactory & factory);
void registerFunctionRandomPrintableASCII(FunctionFactory & factory);
void registerFunctionRandomString(FunctionFactory & factory);
void registerFunctionRandomFixedString(FunctionFactory & factory);
void registerFunctionRandomStringUTF8(FunctionFactory & factory);
void registerFunctionFuzzBits(FunctionFactory & factory);

void registerFunctionsRandom(FunctionFactory & factory)
{
    registerFunctionRand(factory);
    registerFunctionRand64(factory);
    registerFunctionRandConstant(factory);
    registerFunctionGenerateUUIDv4(factory);
    registerFunctionRandomPrintableASCII(factory);
    registerFunctionRandomString(factory);
    registerFunctionRandomFixedString(factory);
    registerFunctionRandomStringUTF8(factory);
    registerFunctionFuzzBits(factory);
}

}
