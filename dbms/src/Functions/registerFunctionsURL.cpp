namespace DB
{

class FunctionFactory;

void registerFunctionProtocol(FunctionFactory &);
void registerFunctionDomain(FunctionFactory &);
void registerFunctionDomainWithoutWWW(FunctionFactory &);
void registerFunctionFirstSignificantSubdomain(FunctionFactory &);
void registerFunctionTopLevelDomain(FunctionFactory &);
void registerFunctionPath(FunctionFactory &);
void registerFunctionPathFull(FunctionFactory &);
void registerFunctionQueryString(FunctionFactory &);
void registerFunctionFragment(FunctionFactory &);
void registerFunctionQueryStringAndFragment(FunctionFactory &);
void registerFunctionExtractURLParameter(FunctionFactory &);
void registerFunctionExtractURLParameters(FunctionFactory &);
void registerFunctionExtractURLParameterNames(FunctionFactory &);
void registerFunctionURLHierarchy(FunctionFactory &);
void registerFunctionURLPathHierarchy(FunctionFactory &);
void registerFunctionCutToFirstSignificantSubdomain(FunctionFactory &);
void registerFunctionCutWWW(FunctionFactory &);
void registerFunctionCutQueryString(FunctionFactory &);
void registerFunctionCutFragment(FunctionFactory &);
void registerFunctionCutQueryStringAndFragment(FunctionFactory &);
void registerFunctionCutURLParameter(FunctionFactory &);
void registerFunctionDecodeURLComponent(FunctionFactory &);

void registerFunctionsURL(FunctionFactory & factory)
{
    registerFunctionProtocol(factory);
    registerFunctionDomain(factory);
    registerFunctionDomainWithoutWWW(factory);
    registerFunctionFirstSignificantSubdomain(factory);
    registerFunctionTopLevelDomain(factory);
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

