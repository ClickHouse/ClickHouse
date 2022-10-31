#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractFloat { static constexpr auto name = "simpleJSONExtractFloat"; };
using FunctionSimpleJSONExtractFloat = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractFloat, ExtractNumericType<Float64>>>;

REGISTER_FUNCTION(VisitParamExtractFloat)
{
    factory.registerFunction<FunctionSimpleJSONExtractFloat>();
    factory.registerAlias("visitParamExtractFloat", "simpleJSONExtractFloat");
}

}
