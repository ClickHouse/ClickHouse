#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "FunctionsURL.h"
#include "path.h"
#include <common/find_symbols.h>

namespace DB
{

struct NamePathFull { static constexpr auto name = "pathFull"; };
using FunctionPathFull = FunctionStringToString<ExtractSubstringImpl<ExtractPath<true>>, NamePathFull>;

void registerFunctionPathFull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPathFull>();
}

}
