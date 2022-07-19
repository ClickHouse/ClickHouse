#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractFloat  { static constexpr auto name = "visitParamExtractFloat"; };
using FunctionVisitParamExtractFloat = FunctionsStringSearch<ExtractParamImpl<NameVisitParamExtractFloat, ExtractNumericType<Float64>>>;

struct NameSimpleJSONExtractFloat  { static constexpr auto name = "simpleJSONExtractFloat"; };
using FunctionSimpleJSONExtractFloat = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractFloat, ExtractNumericType<Float64>>>;

void registerFunctionVisitParamExtractFloat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractFloat>();
    factory.registerFunction<FunctionSimpleJSONExtractFloat>();
}

}
