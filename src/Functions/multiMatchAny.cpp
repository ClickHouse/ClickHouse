#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAny
{
    static constexpr auto name = "multiMatchAny";
};

using FunctionMultiMatchAny = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAny, /*ResultType*/ UInt8, MultiMatchTraits::Find::Any, /*WithEditDistance*/ false>>;

}

REGISTER_FUNCTION(MultiMatchAny)
{
    factory.registerFunction<FunctionMultiMatchAny>();
}

}
