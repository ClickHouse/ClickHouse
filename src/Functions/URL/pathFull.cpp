#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/StringHelpers.h>
#include <Functions/URL/path.h>
#include <base/find_symbols.h>

namespace DB
{

struct NamePathFull { static constexpr auto name = "pathFull"; };
using FunctionPathFull = FunctionStringToString<ExtractSubstringImpl<ExtractPath<true>>, NamePathFull>;

REGISTER_FUNCTION(PathFull)
{
    factory.registerFunction<FunctionPathFull>();
}

}
