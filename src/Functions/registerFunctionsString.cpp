#include "config_functions.h"
#include "config_core.h"

namespace DB
{

class FunctionFactory;

void registerFunctionRepeat(FunctionFactory &);
void registerFunctionEmpty(FunctionFactory &);
void registerFunctionNotEmpty(FunctionFactory &);
void registerFunctionLength(FunctionFactory &);
void registerFunctionLengthUTF8(FunctionFactory &);
void registerFunctionIsValidUTF8(FunctionFactory &);
void registerFunctionToValidUTF8(FunctionFactory &);
void registerFunctionLower(FunctionFactory &);
void registerFunctionUpper(FunctionFactory &);
void registerFunctionLowerUTF8(FunctionFactory &);
void registerFunctionUpperUTF8(FunctionFactory &);
void registerFunctionReverse(FunctionFactory &);
void registerFunctionReverseUTF8(FunctionFactory &);
void registerFunctionsConcat(FunctionFactory &);
void registerFunctionFormat(FunctionFactory &);
void registerFunctionFormatRow(FunctionFactory &);
void registerFunctionSubstring(FunctionFactory &);
void registerFunctionLeft(FunctionFactory &);
void registerFunctionRight(FunctionFactory &);
void registerFunctionCRC(FunctionFactory &);
void registerFunctionAppendTrailingCharIfAbsent(FunctionFactory &);
void registerFunctionStartsWith(FunctionFactory &);
void registerFunctionEndsWith(FunctionFactory &);
void registerFunctionTrim(FunctionFactory &);
void registerFunctionPadString(FunctionFactory &);
void registerFunctionRegexpQuoteMeta(FunctionFactory &);
void registerFunctionNormalizeQuery(FunctionFactory &);
void registerFunctionNormalizedQueryHash(FunctionFactory &);
void registerFunctionCountMatches(FunctionFactory &);
void registerFunctionEncodeXMLComponent(FunctionFactory &);
void registerFunctionDecodeXMLComponent(FunctionFactory &);
void registerFunctionExtractTextFromHTML(FunctionFactory &);
void registerFunctionToStringCutToZero(FunctionFactory &);
void registerFunctionDetectCharset(FunctionFactory &);
void registerFunctionDetectTonality(FunctionFactory &);
void registerFunctionDetectProgrammingLanguage(FunctionFactory &);

#if USE_BASE64
void registerFunctionBase64Encode(FunctionFactory &);
void registerFunctionBase64Decode(FunctionFactory &);
void registerFunctionTryBase64Decode(FunctionFactory &);
#endif

#if USE_BASEX
void registerFunctionBase58Encode(FunctionFactory &);
void registerFunctionBase58Decode(FunctionFactory &);
#endif

#if USE_NLP
void registerFunctionStem(FunctionFactory &);
void registerFunctionSynonyms(FunctionFactory &);
void registerFunctionLemmatize(FunctionFactory &);
void registerFunctionsDetectLanguage(FunctionFactory &);
#endif

#if USE_ICU
void registerFunctionNormalizeUTF8(FunctionFactory &);
#endif

void registerFunctionsString(FunctionFactory & factory)
{
    registerFunctionRepeat(factory);
    registerFunctionEmpty(factory);
    registerFunctionNotEmpty(factory);
    registerFunctionLength(factory);
    registerFunctionLengthUTF8(factory);
    registerFunctionIsValidUTF8(factory);
    registerFunctionToValidUTF8(factory);
    registerFunctionLower(factory);
    registerFunctionUpper(factory);
    registerFunctionLowerUTF8(factory);
    registerFunctionUpperUTF8(factory);
    registerFunctionReverse(factory);
    registerFunctionCRC(factory);
    registerFunctionReverseUTF8(factory);
    registerFunctionsConcat(factory);
    registerFunctionFormat(factory);
    registerFunctionFormatRow(factory);
    registerFunctionSubstring(factory);
    registerFunctionLeft(factory);
    registerFunctionRight(factory);
    registerFunctionAppendTrailingCharIfAbsent(factory);
    registerFunctionStartsWith(factory);
    registerFunctionEndsWith(factory);
    registerFunctionTrim(factory);
    registerFunctionPadString(factory);
    registerFunctionRegexpQuoteMeta(factory);
    registerFunctionNormalizeQuery(factory);
    registerFunctionNormalizedQueryHash(factory);
    registerFunctionCountMatches(factory);
    registerFunctionEncodeXMLComponent(factory);
    registerFunctionDecodeXMLComponent(factory);
    registerFunctionExtractTextFromHTML(factory);
    registerFunctionToStringCutToZero(factory);
    registerFunctionDetectCharset(factory);
    registerFunctionDetectTonality(factory);
    registerFunctionDetectProgrammingLanguage(factory);

#if USE_BASE64
    registerFunctionBase64Encode(factory);
    registerFunctionBase64Decode(factory);
    registerFunctionTryBase64Decode(factory);
#endif

#if USE_BASEX
    registerFunctionBase58Encode(factory);
    registerFunctionBase58Decode(factory);
#endif

#if USE_NLP
    registerFunctionStem(factory);
    registerFunctionSynonyms(factory);
    registerFunctionLemmatize(factory);
    registerFunctionsDetectLanguage(factory);
#endif

#if USE_ICU
    registerFunctionNormalizeUTF8(factory);
#endif
}

}
