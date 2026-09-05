#include <Core/Protocol.h>
#include <base/EnumReflection.h>

namespace DB::Protocol
{

namespace Server
{

std::string_view toString(UInt64 packet)
{
    return magic_enum::enum_name(Enum(packet));
}

}

namespace Client
{

std::string_view toString(UInt64 packet)
{
    return magic_enum::enum_name(Enum(packet));
}

}

}
