#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "queryString.h"

namespace DB
{

struct NameQueryString { static constexpr auto name = "queryString"; };
using FunctionQueryString = FunctionStringToString<ExtractSubstringImpl<ExtractQueryString<true>>, NameQueryString>;

REGISTER_FUNCTION(QueryString)
{
    factory.registerFunction<FunctionQueryString>();
}

}
