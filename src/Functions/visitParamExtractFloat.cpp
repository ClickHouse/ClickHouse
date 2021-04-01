#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractFloat  { static constexpr auto name = "visitParamExtractFloat"; };
using FunctionVisitParamExtractFloat = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Float64>>, NameVisitParamExtractFloat>;

struct NameSimpleJSONExtractFloat  { static constexpr auto name = "simpleJSONExtractFloat"; };
using FunctionSimpleJSONExtractFloat = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Float64>>, NameSimpleJSONExtractFloat>;

void registerFunctionVisitParamExtractFloat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractFloat>();
    factory.registerFunction<FunctionSimpleJSONExtractFloat>();
}

}
