#include <Functions/FunctionFactory.h>
#include <Functions/array/registerFunctionsArray.h>
#include <Functions/registerFunctions.h>

namespace DB
{

void registerFunctions()
{
    auto & factory = FunctionFactory::instance();

    registerFunctionsArithmetic(factory);
    registerFunctionsArray(factory);
    registerFunctionsTuple(factory);
    registerFunctionsBitmap(factory);
    registerFunctionsCoding(factory);
    registerFunctionsComparison(factory);
    registerFunctionsConditional(factory);
    registerFunctionsConversion(factory);
    registerFunctionsDateTime(factory);
    registerFunctionsEmbeddedDictionaries(factory);
    registerFunctionsExternalDictionaries(factory);
    registerFunctionsExternalModels(factory);
    registerFunctionsFormatting(factory);
    registerFunctionsHashing(factory);
    registerFunctionsHigherOrder(factory);
    registerFunctionsLogical(factory);
    registerFunctionsMiscellaneous(factory);
    registerFunctionsRandom(factory);
    registerFunctionsReinterpret(factory);
    registerFunctionsRound(factory);
    registerFunctionsString(factory);
    registerFunctionsStringArray(factory);
    registerFunctionsStringSearch(factory);
    registerFunctionsStringRegex(factory);
    registerFunctionsStringSimilarity(factory);
    registerFunctionsURL(factory);
    registerFunctionsVisitParam(factory);
    registerFunctionsMath(factory);
    registerFunctionsGeo(factory);
    registerFunctionsNull(factory);
    registerFunctionsFindCluster(factory);
    registerFunctionsJSON(factory);
    registerFunctionsIntrospection(factory);
    registerFunctionsConsistentHashing(factory);
}

}
