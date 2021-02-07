#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitive
{
    static constexpr auto name = "multiSearchAnyCaseInsensitive";
};
using FunctionMultiSearchCaseInsensitive
    = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseInsensitiveASCII>, NameMultiSearchAnyCaseInsensitive>;

}

void registerFunctionMultiSearchAnyCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchCaseInsensitive>();
}

}
