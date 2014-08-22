#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsStringSearch.h>

namespace DB
{

void registerFunctionsStringSearch(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("replaceOne", 			F { return new FunctionReplaceOne; });
	factory.registerFunction("replaceAll", 			F { return new FunctionReplaceAll; });
	factory.registerFunction("replaceRegexpOne", 	F { return new FunctionReplaceRegexpOne; });
	factory.registerFunction("replaceRegexpAll", 	F { return new FunctionReplaceRegexpAll; });
	factory.registerFunction("position", 			F { return new FunctionPosition; });
	factory.registerFunction("positionUTF8", 		F { return new FunctionPositionUTF8; });
	factory.registerFunction("match", 				F { return new FunctionMatch; });
	factory.registerFunction("like", 				F { return new FunctionLike; });
	factory.registerFunction("notLike", 			F { return new FunctionNotLike; });
	factory.registerFunction("extract", 			F { return new FunctionExtract; });

	#undef F
}

}
