namespace DB
{
class FunctionFactory;

void registerFunctionExtractOSFromUserAgent(FunctionFactory &);
void registerFunctionExtractBrowserFromUserAgent(FunctionFactory &);

void registerFunctionsUserAgent(FunctionFactory & factory)
{
    registerFunctionExtractOSFromUserAgent(factory);
    registerFunctionExtractBrowserFromUserAgent(factory);
}

}
