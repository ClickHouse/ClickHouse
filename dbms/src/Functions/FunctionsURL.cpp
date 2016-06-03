#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsURL.h>

namespace DB
{

void registerFunctionsURL(FunctionFactory & factory)
{
	factory.registerFunction<FunctionProtocol>();
	factory.registerFunction<FunctionDomain>();
	factory.registerFunction<FunctionDomainWithoutWWW>();
	factory.registerFunction<FunctionFirstSignificantSubdomain>();
	factory.registerFunction<FunctionTopLevelDomain>();
	factory.registerFunction<FunctionPath>();
	factory.registerFunction<FunctionPathFull>();
	factory.registerFunction<FunctionQueryString>();
	factory.registerFunction<FunctionFragment>();
	factory.registerFunction<FunctionQueryStringAndFragment>();
	factory.registerFunction<FunctionExtractURLParameter>();
	factory.registerFunction<FunctionExtractURLParameters>();
	factory.registerFunction<FunctionExtractURLParameterNames>();
	factory.registerFunction<FunctionURLHierarchy>();
	factory.registerFunction<FunctionURLPathHierarchy>();
	factory.registerFunction<FunctionCutToFirstSignificantSubdomain>();
	factory.registerFunction<FunctionCutWWW>();
	factory.registerFunction<FunctionCutQueryString>();
	factory.registerFunction<FunctionCutFragment>();
	factory.registerFunction<FunctionCutQueryStringAndFragment>();
	factory.registerFunction<FunctionCutURLParameter>();
}

}
