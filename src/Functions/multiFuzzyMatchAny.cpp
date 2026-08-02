#include <Functions/FunctionsMultiStringFuzzySearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAnyImpl.h>


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

REGISTER_FUNCTION(MultiFuzzyMatchAny)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
}

}
