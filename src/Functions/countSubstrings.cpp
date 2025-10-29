#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CountSubstringsImpl.h>


namespace DB
{
namespace
{

struct NameCountSubstrings
{
    static constexpr auto name = "countSubstrings";
};

using FunctionCountSubstrings = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstrings, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(CountSubstrings)
{
    factory.registerFunction<FunctionCountSubstrings>({}, FunctionFactory::Case::Insensitive);
}
}
