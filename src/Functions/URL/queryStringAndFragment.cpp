#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/queryStringAndFragment.h>

namespace DB
{

struct NameQueryStringAndFragment { static constexpr auto name = "queryStringAndFragment"; };
using FunctionQueryStringAndFragment = FunctionStringToString<ExtractSubstringImpl<ExtractQueryStringAndFragment<true>>, NameQueryStringAndFragment>;

REGISTER_FUNCTION(QueryStringAndFragment)
{
    factory.registerFunction<FunctionQueryStringAndFragment>();
}

}
