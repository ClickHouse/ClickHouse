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

using FunctionMultiMatchAllIndices = FunctionsMultiStringSearch<MultiMatchAllIndicesImpl<NameMultiMatchAllIndices, /*ResultType*/ UInt64, /*WithEditDistance*/ false>>;

}

REGISTER_FUNCTION(MultiMatchAllIndices)
{
    factory.registerFunction<FunctionMultiMatchAllIndices>();
}

}
