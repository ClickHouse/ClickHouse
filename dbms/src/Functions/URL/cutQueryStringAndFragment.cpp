#include <Functions/URL/registerFunctionsURL.h>
#include <Functions/FunctionFactory.h>
#include "queryStringAndFragment.h"
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameCutQueryStringAndFragment { static constexpr auto name = "cutQueryStringAndFragment"; };
using FunctionCutQueryStringAndFragment = FunctionStringToString<CutSubstringImpl<ExtractQueryStringAndFragment<false>>, NameCutQueryStringAndFragment>;

#pragma GCC diagnostic ignored "-Wmissing-declarations"
void registerFunctionCutQueryStringAndFragment(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCutQueryStringAndFragment>();
}

}
