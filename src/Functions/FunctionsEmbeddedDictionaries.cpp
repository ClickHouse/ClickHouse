#include "FunctionFactory.h"
#include "FunctionsEmbeddedDictionaries.h"


namespace DB
{

REGISTER_FUNCTION(EmbeddedDictionaries)
{
    factory.registerFunction<FunctionRegionToCity>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionToArea>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionToDistrict>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionToCountry>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionToContinent>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionToTopContinent>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionToPopulation>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionIn>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionHierarchy>({}, {.is_deterministic = false});
    factory.registerFunction<FunctionRegionToName>({}, {.is_deterministic = false});
}

}
