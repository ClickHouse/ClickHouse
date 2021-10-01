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
using FunctionVisitParamExtractBool = FunctionsStringSearch<ExtractParamImpl<ExtractBool>, NameVisitParamExtractBool>;


void registerFunctionVisitParamExtractBool(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamExtractBool>();
}

}
