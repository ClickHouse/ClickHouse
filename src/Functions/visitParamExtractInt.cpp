#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractInt    { static constexpr auto name = "visitParamExtractInt"; };
using FunctionVisitParamExtractInt = FunctionsStringSearch<ExtractParamImpl<NameVisitParamExtractInt, ExtractNumericType<Int64>>>;

struct NameSimpleJSONExtractInt    { static constexpr auto name = "simpleJSONExtractInt"; };
using FunctionSimpleJSONExtractInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractInt, ExtractNumericType<Int64>>>;

REGISTER_FUNCTION(VisitParamExtractInt)
{
    factory.registerFunction<FunctionVisitParamExtractInt>();
    factory.registerFunction<FunctionSimpleJSONExtractInt>();
}

}
