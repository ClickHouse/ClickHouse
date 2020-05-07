namespace DB
{
class FunctionFactory;

void registerFunctionExtractOSFromUserAgent(FunctionFactory &);

void registerFunctionsUserAgent(FunctionFactory & factory)
{
    registerFunctionExtractOSFromUserAgent(factory);
}

}
