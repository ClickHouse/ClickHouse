#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{

struct NameMultiFuzzyMatchAny
{
    static constexpr auto name = "multiFuzzyMatchAny";
};

using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<UInt8, true, false, true>,
    NameMultiFuzzyMatchAny,
    std::numeric_limits<UInt32>::max()>;

void registerFunctionMultiFuzzyMatchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
}

}
