#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/protocol.h>


namespace DB
{

struct NameProtocol { static constexpr auto name = "protocol"; };
using FunctionProtocol = FunctionStringToString<ExtractSubstringImpl<ExtractProtocol>, NameProtocol>;

REGISTER_FUNCTION(Protocol)
{
    factory.registerFunction<FunctionProtocol>();
}

}
