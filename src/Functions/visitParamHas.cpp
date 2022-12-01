#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct HasParam
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 *, const UInt8 *)
    {
        return true;
    }
};

struct NameSimpleJSONHas { static constexpr auto name = "simpleJSONHas"; };
using FunctionSimpleJSONHas = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONHas, HasParam>>;

REGISTER_FUNCTION(VisitParamHas)
{
    factory.registerFunction<FunctionSimpleJSONHas>();
    factory.registerAlias("visitParamHas", "simpleJSONHas");
}

}
