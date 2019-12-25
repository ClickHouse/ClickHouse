#include "registerFunctionsURL.h"

namespace DB
{

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

