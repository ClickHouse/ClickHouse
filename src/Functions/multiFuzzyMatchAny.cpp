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

using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<MultiMatchAnyImpl<NameMultiFuzzyMatchAny, /*ResultType*/ UInt8, MultiMatchTraits::Find::Any, /*WithEditDistance*/ true>>;

}

void registerFunctionMultiFuzzyMatchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
}

}
