#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstPositionCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchFirstPositionCaseInsensitiveUTF8";
};

using FunctionMultiSearchFirstPositionCaseInsensitiveUTF8 = FunctionsMultiStringSearch<
    MultiSearchFirstPositionImpl<NameMultiSearchFirstPositionCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

void registerFunctionMultiSearchFirstPositionCaseInsensitiveUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitiveUTF8>();
}

}
