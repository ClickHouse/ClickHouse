namespace DB
{

class FunctionFactory;

void registerFunctionProtocol(FunctionFactory & factory);
void registerFunctionDomain(FunctionFactory & factory);
void registerFunctionDomainWithoutWWW(FunctionFactory & factory);
void registerFunctionFirstSignificantSubdomain(FunctionFactory & factory);
void registerFunctionTopLevelDomain(FunctionFactory & factory);
void registerFunctionPort(FunctionFactory & factory);
void registerFunctionPath(FunctionFactory & factory);
void registerFunctionPathFull(FunctionFactory & factory);
void registerFunctionQueryString(FunctionFactory & factory);
void registerFunctionFragment(FunctionFactory & factory);
void registerFunctionQueryStringAndFragment(FunctionFactory & factory);
void registerFunctionExtractURLParameter(FunctionFactory & factory);
void registerFunctionExtractURLParameters(FunctionFactory & factory);
void registerFunctionExtractURLParameterNames(FunctionFactory & factory);
void registerFunctionURLHierarchy(FunctionFactory & factory);
void registerFunctionURLPathHierarchy(FunctionFactory & factory);
void registerFunctionCutToFirstSignificantSubdomain(FunctionFactory & factory);
void registerFunctionCutWWW(FunctionFactory & factory);
void registerFunctionCutQueryString(FunctionFactory & factory);
void registerFunctionCutFragment(FunctionFactory & factory);
void registerFunctionCutQueryStringAndFragment(FunctionFactory & factory);
void registerFunctionCutURLParameter(FunctionFactory & factory);
void registerFunctionDecodeURLComponent(FunctionFactory & factory);

void registerFunctionsURL(FunctionFactory & factory)
{
    registerFunctionProtocol(factory);
    registerFunctionDomain(factory);
    registerFunctionDomainWithoutWWW(factory);
    registerFunctionFirstSignificantSubdomain(factory);
    registerFunctionTopLevelDomain(factory);
    registerFunctionPort(factory);
    registerFunctionPath(factory);
    registerFunctionPathFull(factory);
    registerFunctionQueryString(factory);
    registerFunctionFragment(factory);
    registerFunctionQueryStringAndFragment(factory);
    registerFunctionExtractURLParameter(factory);
    registerFunctionExtractURLParameters(factory);
    registerFunctionExtractURLParameterNames(factory);
    registerFunctionURLHierarchy(factory);
    registerFunctionURLPathHierarchy(factory);
    registerFunctionCutToFirstSignificantSubdomain(factory);
    registerFunctionCutWWW(factory);
    registerFunctionCutQueryString(factory);
    registerFunctionCutFragment(factory);
    registerFunctionCutQueryStringAndFragment(factory);
    registerFunctionCutURLParameter(factory);
    registerFunctionDecodeURLComponent(factory);
}

}

