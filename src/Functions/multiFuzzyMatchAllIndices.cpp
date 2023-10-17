#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAllIndicesImpl.h"


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAllIndices
{
    static constexpr auto name = "multiFuzzyMatchAllIndices";
};

using FunctionMultiFuzzyMatchAllIndices = FunctionsMultiStringFuzzySearch<MultiMatchAllIndicesImpl<NameMultiFuzzyMatchAllIndices, /*ResultType*/ UInt64, /*WithEditDistance*/ true>>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAllIndices)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAllIndices>();
}

}
