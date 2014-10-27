#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsURL.h>

namespace DB
{

void registerFunctionsURL(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("protocol", 					F { return new FunctionProtocol; });
	factory.registerFunction("domain", 						F { return new FunctionDomain; });
	factory.registerFunction("domainWithoutWWW", 			F { return new FunctionDomainWithoutWWW; });
	factory.registerFunction("firstSignificantSubdomain",	F { return new FunctionFirstSignificantSubdomain; });
	factory.registerFunction("topLevelDomain", 				F { return new FunctionTopLevelDomain; });
	factory.registerFunction("path", 						F { return new FunctionPath; });
	factory.registerFunction("queryString", 				F { return new FunctionQueryString; });
	factory.registerFunction("fragment", 					F { return new FunctionFragment; });
	factory.registerFunction("queryStringAndFragment", 		F { return new FunctionQueryStringAndFragment; });
	factory.registerFunction("extractURLParameter", 		F { return new FunctionExtractURLParameter; });
	factory.registerFunction("extractURLParameters", 		F { return new FunctionExtractURLParameters; });
	factory.registerFunction("extractURLParameterNames", 	F { return new FunctionExtractURLParameterNames; });
	factory.registerFunction("URLHierarchy", 				F { return new FunctionURLHierarchy; });
	factory.registerFunction("URLPathHierarchy", 			F { return new FunctionURLPathHierarchy; });
	factory.registerFunction("cutToFirstSignificantSubdomain", F { return new FunctionCutToFirstSignificantSubdomain; });
	factory.registerFunction("cutWWW", 						F { return new FunctionCutWWW; });
	factory.registerFunction("cutQueryString", 				F { return new FunctionCutQueryString; });
	factory.registerFunction("cutFragment", 				F { return new FunctionCutFragment; });
	factory.registerFunction("cutQueryStringAndFragment", 	F { return new FunctionCutQueryStringAndFragment; });
	factory.registerFunction("cutURLParameter", 			F { return new FunctionCutURLParameter; });

	#undef F
}

}
