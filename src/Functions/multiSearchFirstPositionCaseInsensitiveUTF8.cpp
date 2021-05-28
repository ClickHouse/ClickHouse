#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{

struct NameMultiSearchFirstPositionCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchFirstPositionCaseInsensitiveUTF8";
};

using FunctionMultiSearchFirstPositionCaseInsensitiveUTF8 = FunctionsMultiStringSearch<
    MultiSearchFirstPositionImpl<PositionCaseInsensitiveUTF8>,
    NameMultiSearchFirstPositionCaseInsensitiveUTF8>;

void registerFunctionMultiSearchFirstPositionCaseInsensitiveUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitiveUTF8>();
}

}
