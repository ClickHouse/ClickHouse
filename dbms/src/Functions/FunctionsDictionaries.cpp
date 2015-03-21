#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsDictionaries.h>

namespace DB
{

void registerFunctionsDictionaries(FunctionFactory & factory)
{
	factory.registerFunction<FunctionRegionToCity>();
	factory.registerFunction<FunctionRegionToArea>();
	factory.registerFunction<FunctionRegionToDistrict>();
	factory.registerFunction<FunctionRegionToCountry>();
	factory.registerFunction<FunctionRegionToContinent>();
	factory.registerFunction<FunctionRegionToPopulation>();
	factory.registerFunction<FunctionOSToRoot>();
	factory.registerFunction<FunctionSEToRoot>();
	factory.registerFunction<FunctionRegionIn>();
	factory.registerFunction<FunctionOSIn>();
	factory.registerFunction<FunctionSEIn>();
	factory.registerFunction<FunctionRegionHierarchy>();
	factory.registerFunction<FunctionOSHierarchy>();
	factory.registerFunction<FunctionSEHierarchy>();
	factory.registerFunction<FunctionRegionToName>();
	factory.registerFunction<FunctionDictGetUInt8>();
	factory.registerFunction<FunctionDictGetUInt16>();
	factory.registerFunction<FunctionDictGetUInt32>();
	factory.registerFunction<FunctionDictGetUInt64>();
	factory.registerFunction<FunctionDictGetInt8>();
	factory.registerFunction<FunctionDictGetInt16>();
	factory.registerFunction<FunctionDictGetInt32>();
	factory.registerFunction<FunctionDictGetInt64>();
	factory.registerFunction<FunctionDictGetFloat32>();
	factory.registerFunction<FunctionDictGetFloat64>();
	factory.registerFunction<FunctionDictGetDate>();
	factory.registerFunction<FunctionDictGetDateTime>();
	factory.registerFunction<FunctionDictGetString>();
	factory.registerFunction<FunctionDictGetHierarchy>();
	factory.registerFunction<FunctionDictIsIn>();
}
	
}
