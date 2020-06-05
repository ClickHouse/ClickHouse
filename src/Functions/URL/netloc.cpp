#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "netloc.h"

namespace DB
{

struct NameNetloc { static constexpr auto name = "netloc"; };
using FunctionNetloc = FunctionStringToString<ExtractSubstringImpl<ExtractNetloc>, NameNetloc>;

void registerFunctionNetloc(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNetloc>();
}

}

