#pragma once

#include <cstddef>

namespace DB
{

// Version of protocol between clickhouse-server and clickhouse-library-bridge. Increment if you change it in a non-compatible way.
static constexpr size_t LIBRARY_BRIDGE_PROTOCOL_VERSION = 1;

// Version of protocol between clickhouse-server and clickhouse-xdbc-bridge. Increment if you change it in a non-compatible way.
static constexpr size_t XDBC_BRIDGE_PROTOCOL_VERSION = 1;

}
