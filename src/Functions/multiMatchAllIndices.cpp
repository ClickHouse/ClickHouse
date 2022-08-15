#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAllIndicesImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAllIndices
{
    static constexpr auto name = "multiMatchAllIndices";
};

using FunctionMultiMatchAllIndices = FunctionsMultiStringSearch<
    MultiMatchAllIndicesImpl<UInt64, false>,
    NameMultiMatchAllIndices,
    std::numeric_limits<UInt32>::max()>;

}

void registerFunctionMultiMatchAllIndices(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiMatchAllIndices>();
}

}
