#pragma once

#include <cstddef>

namespace DB
{

// Version of protocol between clickhouse-server and clickhouse-xdbc-bridge. Increment if you change it in a non-compatible way.
static constexpr size_t XDBC_BRIDGE_PROTOCOL_VERSION = 1;

/// The query parameter in which a bridge request carries the whole connection string, credentials included.
static constexpr auto XDBC_BRIDGE_CONNECTION_STRING_PARAMETER = "connection_string";

}
