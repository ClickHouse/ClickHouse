#pragma once

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

constexpr UInt16 MIN_PORT_NUMBER = 1;
constexpr UInt16 MAX_PORT_NUMBER = 65535;

/// Apply port offset and validate the result is within valid range
inline UInt16 applyPortOffset(UInt16 port, Int32 offset)
{
    if (offset == 0)
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

}
