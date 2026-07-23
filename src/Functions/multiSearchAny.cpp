#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAny
{
    static constexpr auto name = "multiSearchAny";
};

using FunctionMultiSearch = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAny, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAny)
{
    factory.registerFunction<FunctionMultiSearch>();
}

}
