#include "config_core.h"

#include <Functions/FunctionFactory.h>


namespace DB
{
void registerFunctionsArithmetic(FunctionFactory &);
void registerFunctionsArray(FunctionFactory &);
void registerFunctionsTuple(FunctionFactory &);
void registerFunctionsMakeDate(FunctionFactory &);
void registerFunctionsMap(FunctionFactory &);
void registerFunctionsBitmap(FunctionFactory &);
void registerFunctionsBinaryRepr(FunctionFactory &);
void registerFunctionsCoding(FunctionFactory &);
void registerFunctionsCodingUUID(FunctionFactory &);
void registerFunctionChar(FunctionFactory &);
void registerFunctionsComparison(FunctionFactory &);
void registerFunctionsConditional(FunctionFactory &);
void registerFunctionsConversion(FunctionFactory &);
void registerFunctionCastOrDefault(FunctionFactory &);
void registerFunctionsDateTime(FunctionFactory &);
void registerFunctionsEmbeddedDictionaries(FunctionFactory &);
void registerFunctionsExternalDictionaries(FunctionFactory &);
void registerFunctionsExternalModels(FunctionFactory &);
void registerFunctionsFormatting(FunctionFactory &);
void registerFunctionHashID(FunctionFactory &);
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
void registerFunctionsStringTokenExtractor(FunctionFactory &);
void registerFunctionsURL(FunctionFactory &);
void registerFunctionsVisitParam(FunctionFactory &);
void registerFunctionsMath(FunctionFactory &);
void registerFunctionsGeo(FunctionFactory &);
void registerFunctionsIntrospection(FunctionFactory &);
void registerFunctionsNull(FunctionFactory &);
void registerFunctionsJSON(FunctionFactory &);
void registerFunctionsSQLJSON(FunctionFactory &);
void registerFunctionToJSONString(FunctionFactory &);
void registerFunctionsConsistentHashing(FunctionFactory & factory);
void registerFunctionsUnixTimestamp64(FunctionFactory & factory);
void registerFunctionBitHammingDistance(FunctionFactory & factory);
void registerFunctionTupleHammingDistance(FunctionFactory & factory);
void registerFunctionsStringHash(FunctionFactory & factory);
void registerFunctionValidateNestedArraySizes(FunctionFactory & factory);
void registerFunctionsSnowflake(FunctionFactory & factory);
void registerFunctionTid(FunctionFactory & factory);
void registerFunctionLogTrace(FunctionFactory & factory);
void registerFunctionsTimeWindow(FunctionFactory &);
void registerFunctionToBool(FunctionFactory &);
void registerFunctionMinSampleSize(FunctionFactory &);

/// For Meilisearch
void registerFunctionMeiliMatch(FunctionFactory & factory);

#if USE_SSL
void registerFunctionEncrypt(FunctionFactory & factory);
void registerFunctionDecrypt(FunctionFactory & factory);
void registerFunctionAESEncryptMysql(FunctionFactory & factory);
void registerFunctionAESDecryptMysql(FunctionFactory & factory);
void registerFunctionShowCertificate(FunctionFactory &);
#endif

void registerFunctions()
{
    auto & factory = FunctionFactory::instance();

    registerFunctionsArithmetic(factory);
    registerFunctionsArray(factory);
    registerFunctionsTuple(factory);
    registerFunctionsMakeDate(factory);
    registerFunctionsMap(factory);
    registerFunctionsBitmap(factory);
    registerFunctionsBinaryRepr(factory);
    registerFunctionsCoding(factory);
    registerFunctionsCodingUUID(factory);
    registerFunctionChar(factory);
    registerFunctionsComparison(factory);
    registerFunctionsConditional(factory);
    registerFunctionsConversion(factory);
    registerFunctionCastOrDefault(factory);
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
    registerFunctionsStringTokenExtractor(factory);
    registerFunctionsURL(factory);
    registerFunctionsVisitParam(factory);
    registerFunctionsMath(factory);
    registerFunctionsGeo(factory);
    registerFunctionsNull(factory);
    registerFunctionsJSON(factory);
    registerFunctionsSQLJSON(factory);
    registerFunctionToJSONString(factory);
    registerFunctionsIntrospection(factory);
    registerFunctionsConsistentHashing(factory);
    registerFunctionsUnixTimestamp64(factory);
    registerFunctionBitHammingDistance(factory);
    registerFunctionTupleHammingDistance(factory);
    registerFunctionsStringHash(factory);
    registerFunctionValidateNestedArraySizes(factory);
    registerFunctionsSnowflake(factory);
    registerFunctionsTimeWindow(factory);
    registerFunctionToBool(factory);
    registerFunctionMinSampleSize(factory);
    registerFunctionTid(factory);
    registerFunctionLogTrace(factory);
    registerFunctionHashID(factory);

    /// For Meilisearch
    registerFunctionMeiliMatch(factory);

#if USE_SSL
    registerFunctionEncrypt(factory);
    registerFunctionDecrypt(factory);
    registerFunctionAESEncryptMysql(factory);
    registerFunctionAESDecryptMysql(factory);
    registerFunctionShowCertificate(factory);
#endif
}

}
