#include <Core/Protocol.h>
#include <base/EnumReflection.h>

namespace DB::Protocol
{

namespace Server
{

std::string_view toString(UInt64 packet)
{
    /// packet comes straight off the wire and may be out of range (stream desync,
    /// fuzzing). Loading an out-of-range value into Enum is undefined behavior, so
    /// reject it before the cast; magic_enum already maps unknown values to "".
    if (packet > MAX)
        return {};
    return magic_enum::enum_name(Enum(packet));
}

}

namespace Client
{

std::string_view toString(UInt64 packet)
{
    if (packet > MAX)
        return {};
    return magic_enum::enum_name(Enum(packet));
}

}

}
