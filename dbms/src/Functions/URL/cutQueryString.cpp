#include <Functions/URL/registerFunctionsURL.h>
#include <Functions/FunctionFactory.h>
#include "queryString.h"
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameCutQueryString { static constexpr auto name = "cutQueryString"; };
using FunctionCutQueryString = FunctionStringToString<CutSubstringImpl<ExtractQueryString<false>>, NameCutQueryString>;

#pragma GCC diagnostic ignored "-Wmissing-declarations"
void registerFunctionCutQueryString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCutQueryString>();
}

}
