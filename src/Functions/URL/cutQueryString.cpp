#include <Functions/FunctionFactory.h>
#include "queryString.h"
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameCutQueryString { static constexpr auto name = "cutQueryString"; };
using FunctionCutQueryString = FunctionStringToString<CutSubstringImpl<ExtractQueryString<false>>, NameCutQueryString>;

void registerFunctionCutQueryString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCutQueryString>();
}

}
