#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractFloat  { static constexpr auto name = "visitParamExtractFloat"; };
using FunctionVisitParamExtractFloat = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Float64>>, NameVisitParamExtractFloat>;

struct NameJSONSExtractFloat  { static constexpr auto name = "JSONSExtractFloat"; };
using FunctionJSONSExtractFloat = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Float64>>, NameJSONSExtractFloat>;

void registerFunctionVisitParamExtractFloat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractFloat>();
    factory.registerFunction<FunctionJSONSExtractFloat>();
}

}
