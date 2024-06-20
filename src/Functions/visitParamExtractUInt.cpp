#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractUInt { static constexpr auto name = "simpleJSONExtractUInt"; };
using FunctionSimpleJSONExtractUInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractUInt, ExtractNumericType<UInt64>>>;


REGISTER_FUNCTION(VisitParamExtractUInt)
{
    factory.registerFunction<FunctionSimpleJSONExtractUInt>();
    factory.registerAlias("visitParamExtractUInt", "simpleJSONExtractUInt");
}

}
