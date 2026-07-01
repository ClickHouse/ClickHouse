#include <Core/Protocol.h>
#include <base/EnumReflection.h>

namespace DB::Protocol
{

namespace Server
{

std::string toString(UInt64 packet)
{
    /// packet comes straight off the wire and may be out of range (stream desync,
    /// fuzzing). Loading an out-of-range value into Enum is undefined behavior, so
    /// reject it before the cast and fall back to the numeric value, which keeps the
    /// "unexpected packet" error messages debuggeable.
    if (packet > MAX)
        return std::to_string(packet);
    return std::string(magic_enum::enum_name(Enum(packet)));
}

}

namespace Client
{

std::string toString(UInt64 packet)
{
    if (packet > MAX)
        return std::to_string(packet);
    return std::string(magic_enum::enum_name(Enum(packet)));
}

}

}
