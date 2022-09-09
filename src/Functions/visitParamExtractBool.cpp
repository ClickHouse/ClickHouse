#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct ExtractBool
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 * begin, const UInt8 * end)
    {
        return begin + 4 <= end && 0 == strncmp(reinterpret_cast<const char *>(begin), "true", 4);
    }
};

struct NameVisitParamExtractBool   { static constexpr auto name = "visitParamExtractBool"; };
using FunctionVisitParamExtractBool = FunctionsStringSearch<ExtractParamImpl<NameVisitParamExtractBool, ExtractBool>>;

struct NameSimpleJSONExtractBool   { static constexpr auto name = "simpleJSONExtractBool"; };
using FunctionSimpleJSONExtractBool = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractBool, ExtractBool>>;

REGISTER_FUNCTION(VisitParamExtractBool)
{
    factory.registerFunction<FunctionVisitParamExtractBool>();
    factory.registerFunction<FunctionSimpleJSONExtractBool>();
}

}
