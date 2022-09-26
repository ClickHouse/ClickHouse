#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAny
{
    static constexpr auto name = "multiFuzzyMatchAny";
};

using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<NameMultiFuzzyMatchAny, UInt8, true, false, true>,
    std::numeric_limits<UInt32>::max()>;

}

void registerFunctionMultiFuzzyMatchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
}

}
