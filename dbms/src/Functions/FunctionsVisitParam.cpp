#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionsString.h>
#include <Functions/FunctionsURL.h>


namespace DB
{

struct NameVisitParamHas           { static constexpr auto name = "visitParamHas"; };
struct NameVisitParamExtractUInt   { static constexpr auto name = "visitParamExtractUInt"; };
struct NameVisitParamExtractInt    { static constexpr auto name = "visitParamExtractInt"; };
struct NameVisitParamExtractFloat  { static constexpr auto name = "visitParamExtractFloat"; };
struct NameVisitParamExtractBool   { static constexpr auto name = "visitParamExtractBool"; };
struct NameVisitParamExtractRaw    { static constexpr auto name = "visitParamExtractRaw"; };
struct NameVisitParamExtractString { static constexpr auto name = "visitParamExtractString"; };


using FunctionVisitParamHas = FunctionsStringSearch<ExtractParamImpl<HasParam>, NameVisitParamHas>;
using FunctionVisitParamExtractUInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<UInt64>>, NameVisitParamExtractUInt>;
using FunctionVisitParamExtractInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Int64>>, NameVisitParamExtractInt>;
using FunctionVisitParamExtractFloat = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Float64>>, NameVisitParamExtractFloat>;
using FunctionVisitParamExtractBool = FunctionsStringSearch<ExtractParamImpl<ExtractBool>, NameVisitParamExtractBool>;
using FunctionVisitParamExtractRaw = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractRaw>, NameVisitParamExtractRaw>;
using FunctionVisitParamExtractString = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractString>, NameVisitParamExtractString>;



void registerFunctionsVisitParam(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamHas>();
    factory.registerFunction<FunctionVisitParamExtractUInt>();
    factory.registerFunction<FunctionVisitParamExtractInt>();
    factory.registerFunction<FunctionVisitParamExtractFloat>();
    factory.registerFunction<FunctionVisitParamExtractBool>();
    factory.registerFunction<FunctionVisitParamExtractRaw>();
    factory.registerFunction<FunctionVisitParamExtractString>();
}

}
