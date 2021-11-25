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
    MultiMatchAnyImpl<UInt64, false, true, true>,
    NameMultiFuzzyMatchAnyIndex,
    std::numeric_limits<UInt32>::max()>;

}

void registerFunctionMultiFuzzyMatchAnyIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>();
}

}
