#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitive
{
    static constexpr auto name = "multiSearchAnyCaseInsensitive";
};
using FunctionMultiSearchCaseInsensitive = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAnyCaseInsensitive)
{
    factory.registerFunction<FunctionMultiSearchCaseInsensitive>();
}

}
