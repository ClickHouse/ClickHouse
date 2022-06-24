#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAnyIndex
{
    static constexpr auto name = "multiMatchAnyIndex";
};

using FunctionMultiMatchAnyIndex = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAnyIndex, UInt64, false, true, false>>;

}

void registerFunctionMultiMatchAnyIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiMatchAnyIndex>();
}

}
