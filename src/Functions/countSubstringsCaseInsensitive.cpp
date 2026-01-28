#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CountSubstringsImpl.h>


namespace DB
{
namespace
{

struct NameCountSubstringsCaseInsensitive
{
    static constexpr auto name = "countSubstringsCaseInsensitive";
};

using FunctionCountSubstringsCaseInsensitive = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstringsCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitive)
{
    factory.registerFunction<FunctionCountSubstringsCaseInsensitive>();
}
}
