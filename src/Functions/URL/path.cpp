#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "FunctionsURL.h"
#include "path.h"
#include <common/find_symbols.h>


namespace DB
{

struct NamePath { static constexpr auto name = "path"; };
using FunctionPath = FunctionStringToString<ExtractSubstringImpl<ExtractPath<false>>, NamePath>;

void registerFunctionPath(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPath>();
}

}
