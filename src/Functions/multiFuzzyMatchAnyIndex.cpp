#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAnyIndex
{
    static constexpr auto name = "multiFuzzyMatchAnyIndex";
};

using FunctionMultiFuzzyMatchAnyIndex = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<NameMultiFuzzyMatchAnyIndex, UInt64, false, true, true>,
    std::numeric_limits<UInt32>::max()>;

}

void registerFunctionMultiFuzzyMatchAnyIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>();
}

}
