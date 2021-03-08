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

struct NameVisitParamHas           { static constexpr auto name = "visitParamHas"; };
using FunctionVisitParamHas = FunctionsStringSearch<ExtractParamImpl<HasParam>, NameVisitParamHas>;

struct NameJSONSHas           { static constexpr auto name = "JSONSHas"; };
using FunctionJSONSHas = FunctionsStringSearch<ExtractParamImpl<HasParam>, NameJSONSHas>;

void registerFunctionVisitParamHas(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamHas>();
    factory.registerFunction<FunctionJSONSHas>();
}

}
