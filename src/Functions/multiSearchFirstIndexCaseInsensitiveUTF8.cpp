#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchFirstIndexCaseInsensitiveUTF8";
};

using FunctionMultiSearchFirstIndexCaseInsensitiveUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndexCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstIndexCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitiveUTF8>();
}

}
