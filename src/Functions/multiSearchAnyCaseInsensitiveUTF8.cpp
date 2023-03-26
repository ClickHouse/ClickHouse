#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAnyCaseInsensitiveUTF8";
};

using FunctionMultiSearchCaseInsensitiveUTF8
    = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

void registerFunctionMultiSearchAnyCaseInsensitiveUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchCaseInsensitiveUTF8>();
}

}
