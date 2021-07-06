#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractUInt   { static constexpr auto name = "visitParamExtractUInt"; };
using FunctionVisitParamExtractUInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<UInt64>>, NameVisitParamExtractUInt>;

struct NameSimpleJSONExtractUInt   { static constexpr auto name = "simpleJSONExtractUInt"; };
using FunctionSimpleJSONExtractUInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<UInt64>>, NameSimpleJSONExtractUInt>;


void registerFunctionVisitParamExtractUInt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractUInt>();
    factory.registerFunction<FunctionSimpleJSONExtractUInt>();
}

}
