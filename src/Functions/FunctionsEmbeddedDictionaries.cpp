#include "FunctionFactory.h"
#include "FunctionsEmbeddedDictionaries.h"


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
    factory.registerFunction<FunctionRegionIn>();
    factory.registerFunction<FunctionRegionHierarchy>();
    factory.registerFunction<FunctionRegionToName>();
}

}
