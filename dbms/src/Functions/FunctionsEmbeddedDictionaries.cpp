#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsEmbeddedDictionaries.h>

namespace DB
{

void registerFunctionsEmbeddedDictionaries(FunctionFactory & factory)
{
	factory.registerFunction<FunctionRegionToCity>();
	factory.registerFunction<FunctionRegionToArea>();
	factory.registerFunction<FunctionRegionToDistrict>();
	factory.registerFunction<FunctionRegionToCountry>();
	factory.registerFunction<FunctionRegionToContinent>();
	factory.registerFunction<FunctionRegionToTopContinent>();
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
}

}
