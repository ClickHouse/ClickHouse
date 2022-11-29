#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractInt { static constexpr auto name = "simpleJSONExtractInt"; };
using FunctionSimpleJSONExtractInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractInt, ExtractNumericType<Int64>>>;

REGISTER_FUNCTION(VisitParamExtractInt)
{
    factory.registerFunction<FunctionSimpleJSONExtractInt>();
    factory.registerAlias("visitParamExtractInt", "simpleJSONExtractInt");
}

}
