#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{

struct NameMultiMatchAnyIndex
{
    static constexpr auto name = "multiMatchAnyIndex";
};

using FunctionMultiMatchAnyIndex = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<UInt64, false, true, false>,
    NameMultiMatchAnyIndex,
    std::numeric_limits<UInt32>::max()>;

void registerFunctionMultiMatchAnyIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiMatchAnyIndex>();
}

}
