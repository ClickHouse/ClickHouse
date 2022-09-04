#include <Functions/FunctionFactory.h>
#include "queryString.h"
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameCutQueryString { static constexpr auto name = "cutQueryString"; };
using FunctionCutQueryString = FunctionStringToString<CutSubstringImpl<ExtractQueryString<false>>, NameCutQueryString>;

REGISTER_FUNCTION(CutQueryString)
{
    factory.registerFunction<FunctionCutQueryString>();
}

}
