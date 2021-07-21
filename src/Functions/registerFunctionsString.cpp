#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

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


#if USE_BASE64
void registerFunctionBase64Encode(FunctionFactory &);
void registerFunctionBase64Decode(FunctionFactory &);
void registerFunctionTryBase64Decode(FunctionFactory &);
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
#if USE_BASE64
    registerFunctionBase64Encode(factory);
    registerFunctionBase64Decode(factory);
    registerFunctionTryBase64Decode(factory);
#endif
}

}
