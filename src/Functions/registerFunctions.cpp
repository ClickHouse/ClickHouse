#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#include <Functions/FunctionFactory.h>


namespace DB
{
void registerFunctionsArithmetic(FunctionFactory &);
void registerFunctionsArray(FunctionFactory &);
void registerFunctionsTuple(FunctionFactory &);
void registerFunctionsBitmap(FunctionFactory &);
void registerFunctionsCoding(FunctionFactory &);
void registerFunctionsComparison(FunctionFactory &);
void registerFunctionsConditional(FunctionFactory &);
void registerFunctionsConversion(FunctionFactory &);
void registerFunctionsDateTime(FunctionFactory &);
void registerFunctionsEmbeddedDictionaries(FunctionFactory &);
void registerFunctionsExternalDictionaries(FunctionFactory &);
void registerFunctionsExternalModels(FunctionFactory &);
void registerFunctionsFormatting(FunctionFactory &);
void registerFunctionsHashing(FunctionFactory &);
void registerFunctionsHigherOrder(FunctionFactory &);
void registerFunctionsLogical(FunctionFactory &);
void registerFunctionsMiscellaneous(FunctionFactory &);
void registerFunctionsRandom(FunctionFactory &);
void registerFunctionsReinterpret(FunctionFactory &);
void registerFunctionsRound(FunctionFactory &);
void registerFunctionsString(FunctionFactory &);
void registerFunctionsStringArray(FunctionFactory &);
void registerFunctionsStringSearch(FunctionFactory &);
void registerFunctionsStringRegexp(FunctionFactory &);
void registerFunctionsStringSimilarity(FunctionFactory &);
void registerFunctionsURL(FunctionFactory &);
void registerFunctionsVisitParam(FunctionFactory &);
void registerFunctionsMath(FunctionFactory &);
void registerFunctionsGeo(FunctionFactory &);
void registerFunctionsIntrospection(FunctionFactory &);
void registerFunctionsNull(FunctionFactory &);
void registerFunctionsJSON(FunctionFactory &);
void registerFunctionsConsistentHashing(FunctionFactory & factory);
void registerFunctionsUnixTimestamp64(FunctionFactory & factory);

#if !defined(ARCADIA_BUILD)
void registerFunctionBayesAB(FunctionFactory &);
#endif
void registerFunctionTid(FunctionFactory & factory);
void registerFunctionLogTrace(FunctionFactory & factory);

#if USE_SSL
void registerFunctionEncrypt(FunctionFactory & factory);
void registerFunctionDecrypt(FunctionFactory & factory);
void registerFunctionAESEncryptMysql(FunctionFactory & factory);
void registerFunctionAESDecryptMysql(FunctionFactory & factory);

#endif


void registerFunctions()
{
    auto & factory = FunctionFactory::instance();

    registerFunctionsArithmetic(factory);
    registerFunctionsArray(factory);
    registerFunctionsTuple(factory);
#if !defined(ARCADIA_BUILD)
    registerFunctionsBitmap(factory);
#endif
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
    registerFunctionsStringRegexp(factory);
    registerFunctionsStringSimilarity(factory);
    registerFunctionsURL(factory);
    registerFunctionsVisitParam(factory);
    registerFunctionsMath(factory);
    registerFunctionsGeo(factory);
    registerFunctionsNull(factory);
    registerFunctionsJSON(factory);
    registerFunctionsIntrospection(factory);
    registerFunctionsConsistentHashing(factory);
    registerFunctionsUnixTimestamp64(factory);

#if !defined(ARCADIA_BUILD)
    registerFunctionBayesAB(factory);
#endif

#if USE_SSL
    registerFunctionEncrypt(factory);
    registerFunctionDecrypt(factory);
    registerFunctionAESEncryptMysql(factory);
    registerFunctionAESDecryptMysql(factory);
#endif
    registerFunctionTid(factory);
    registerFunctionLogTrace(factory);
}

}
