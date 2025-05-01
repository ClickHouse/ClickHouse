#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


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
