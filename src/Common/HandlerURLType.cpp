#include <Common/HandlerURLType.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

const char * handlerURLTypeToString(HandlerURLType type)
{
    switch (type)
    {
        case HandlerURLType::Exact: return "exact";
        case HandlerURLType::Prefix: return "prefix";
        case HandlerURLType::Regexp: return "regexp";
    }
    UNREACHABLE();
}

HandlerURLType parseHandlerURLType(const std::string & str)
{
    if (str == "exact") return HandlerURLType::Exact;
    if (str == "prefix") return HandlerURLType::Prefix;
    if (str == "regexp") return HandlerURLType::Regexp;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown handler URL type '{}'", str);
}

}
