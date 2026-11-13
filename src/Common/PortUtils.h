#pragma once

#include <Common/Exception.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

constexpr UInt16 MIN_PORT_NUMBER = 1;
constexpr UInt16 MAX_PORT_NUMBER = 65535;

/// Apply port offset and validate the result is within valid range.
/// A zero port means "unset" or an OS-assigned (ephemeral) port, so it is
/// returned unchanged and never offset.
inline UInt16 applyPortOffset(UInt16 port, Int32 offset)
{
    if (offset == 0 || port == 0)
        return port;

    Int64 effective_port = static_cast<Int64>(port) + offset;
    if (effective_port < MIN_PORT_NUMBER || effective_port > MAX_PORT_NUMBER)
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Port {} with offset {} results in invalid port {}: must be in range {}-{}",
            port,
            offset,
            effective_port,
            MIN_PORT_NUMBER,
            MAX_PORT_NUMBER);

    return static_cast<UInt16>(effective_port);
}

/// Read `port_offset` from a raw configuration (0 when absent), rejecting values that do not fit
/// the `Int32` type of the setting. A plain `static_cast<Int32>(config.getInt64(...))` would wrap
/// silently, so e.g. `4294967297` would turn into an offset of `1` and the process would bind or
/// connect to the wrong port instead of refusing the configuration. Use it everywhere the offset is
/// read from the configuration directly rather than through `ServerSettings`.
Int32 getPortOffsetFromConfig(const Poco::Util::AbstractConfiguration & config);

}
