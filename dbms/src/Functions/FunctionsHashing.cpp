#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsHashing.h>


namespace DB
{

void registerFunctionsHashing(FunctionFactory & factory)
{
	factory.registerFunction<FunctionHalfMD5>();
	factory.registerFunction<FunctionMD5>();
	factory.registerFunction<FunctionSHA1>();
	factory.registerFunction<FunctionSHA224>();
	factory.registerFunction<FunctionSHA256>();
	factory.registerFunction<FunctionSipHash64>();
	factory.registerFunction<FunctionSipHash128>();
	factory.registerFunction<FunctionCityHash64>();
	factory.registerFunction<FunctionFarmHash64>();
	factory.registerFunction<FunctionMetroHash64>();
	factory.registerFunction<FunctionIntHash32>();
	factory.registerFunction<FunctionIntHash64>();
	factory.registerFunction<FunctionURLHash>();
}

}
