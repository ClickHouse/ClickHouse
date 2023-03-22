#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "protocol.h"


namespace DB
{

struct NameProtocol { static constexpr auto name = "protocol"; };
using FunctionProtocol = FunctionStringToString<ExtractSubstringImpl<ExtractProtocol>, NameProtocol>;

void registerFunctionProtocol(FunctionFactory & factory)
{
    factory.registerFunction<FunctionProtocol>();
}

}
