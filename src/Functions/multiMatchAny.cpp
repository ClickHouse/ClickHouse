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

using FunctionMultiMatchAny = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAny, UInt8, true, false, false>>;

}

void registerFunctionMultiMatchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiMatchAny>();
}

}
