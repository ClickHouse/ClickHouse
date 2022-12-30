#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameVisitParamExtractUInt   { static constexpr auto name = "visitParamExtractUInt"; };
using FunctionVisitParamExtractUInt = FunctionsStringSearch<ExtractParamImpl<NameVisitParamExtractUInt, ExtractNumericType<UInt64>>>;

struct NameSimpleJSONExtractUInt   { static constexpr auto name = "simpleJSONExtractUInt"; };
using FunctionSimpleJSONExtractUInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractUInt, ExtractNumericType<UInt64>>>;


REGISTER_FUNCTION(VisitParamExtractUInt)
{
    factory.registerFunction<FunctionVisitParamExtractUInt>();
    factory.registerFunction<FunctionSimpleJSONExtractUInt>();
}

}
