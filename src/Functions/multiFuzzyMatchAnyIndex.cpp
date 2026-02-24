#include <Functions/FunctionsMultiStringFuzzySearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAnyImpl.h>


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAnyIndex
{
    static constexpr auto name = "multiFuzzyMatchAnyIndex";
};

using FunctionMultiFuzzyMatchAnyIndex = FunctionsMultiStringFuzzySearch<MultiMatchAnyImpl<NameMultiFuzzyMatchAnyIndex, /*ResultType*/ UInt64, MultiMatchTraits::Find::AnyIndex, /*WithEditDistance*/ true>>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAnyIndex)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>();
}

}
