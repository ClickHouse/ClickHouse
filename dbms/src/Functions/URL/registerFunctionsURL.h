#pragma once

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

void registerFunctionsURL(FunctionFactory &);

}
