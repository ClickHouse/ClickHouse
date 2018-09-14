#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionEmpty(FunctionFactory &);
void registerFunctionNotEmpty(FunctionFactory &);
void registerFunctionLength(FunctionFactory &);
void registerFunctionLengthUTF8(FunctionFactory &);
void registerFunctionLower(FunctionFactory &);
void registerFunctionUpper(FunctionFactory &);
void registerFunctionLowerUTF8(FunctionFactory &);
void registerFunctionUpperUTF8(FunctionFactory &);
void registerFunctionReverse(FunctionFactory &);
void registerFunctionReverseUTF8(FunctionFactory &);
void registerFunctionsConcat(FunctionFactory &);
void registerFunctionSubstring(FunctionFactory &);
void registerFunctionSubstringUTF8(FunctionFactory &);
void registerFunctionAppendTrailingCharIfAbsent(FunctionFactory &);
void registerFunctionStartsWith(FunctionFactory &);
void registerFunctionEndsWith(FunctionFactory &);

void registerFunctionsString(FunctionFactory & factory)
{
    registerFunctionEmpty(factory);
    registerFunctionNotEmpty(factory);
    registerFunctionLength(factory);
    registerFunctionLengthUTF8(factory);
    registerFunctionLower(factory);
    registerFunctionUpper(factory);
    registerFunctionLowerUTF8(factory);
    registerFunctionUpperUTF8(factory);
    registerFunctionReverse(factory);
    registerFunctionReverseUTF8(factory);
    registerFunctionsConcat(factory);
    registerFunctionSubstring(factory);
    registerFunctionSubstringUTF8(factory);
    registerFunctionAppendTrailingCharIfAbsent(factory);
    registerFunctionStartsWith(factory);
    registerFunctionEndsWith(factory);
}

}

