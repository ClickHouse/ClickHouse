#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractInt    { static constexpr auto name = "visitParamExtractInt"; };
using FunctionVisitParamExtractInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Int64>>, NameVisitParamExtractInt>;

struct NameSimpleJSONExtractInt    { static constexpr auto name = "simpleJSONExtractInt"; };
using FunctionSimpleJSONExtractInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Int64>>, NameSimpleJSONExtractInt>;

void registerFunctionVisitParamExtractInt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractInt>();
    factory.registerFunction<FunctionSimpleJSONExtractInt>();
}

}
