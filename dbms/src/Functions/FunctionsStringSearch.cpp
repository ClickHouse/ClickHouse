#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsStringSearch.h>

namespace DB
{

void registerFunctionsStringSearch(FunctionFactory & factory)
{
	factory.registerFunction<FunctionReplaceOne>();
	factory.registerFunction<FunctionReplaceAll>();
	factory.registerFunction<FunctionReplaceRegexpOne>();
	factory.registerFunction<FunctionReplaceRegexpAll>();
	factory.registerFunction<FunctionPosition>();
	factory.registerFunction<FunctionPositionUTF8>();
	factory.registerFunction<FunctionPositionCaseInsensitive>();
	factory.registerFunction<FunctionPositionCaseInsensitiveUTF8>();
	factory.registerFunction<FunctionMatch>();
	factory.registerFunction<FunctionLike>();
	factory.registerFunction<FunctionNotLike>();
	factory.registerFunction<FunctionExtract>();
}

}
