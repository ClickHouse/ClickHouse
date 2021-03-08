#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractInt    { static constexpr auto name = "visitParamExtractInt"; };
using FunctionVisitParamExtractInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Int64>>, NameVisitParamExtractInt>;

struct NameJSONSExtractInt    { static constexpr auto name = "JSONSExtractInt"; };
using FunctionJSONSExtractInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Int64>>, NameJSONSExtractInt>;

void registerFunctionVisitParamExtractInt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractInt>();
    factory.registerFunction<FunctionJSONSExtractInt>();
}

}
