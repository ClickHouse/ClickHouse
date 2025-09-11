#pragma once

namespace DB
{

/// Protocol categories used to expose thread/connection metrics.
/// This is intentionally decoupled from configuration keys and server names.
enum class ProtocolMetricsType
{
    UNKNOWN = 0,

    // ClickHouse server protocols
    TCP,
    TCP_SECURE,
    TCP_WITH_PROXY,
    TCP_SSH,
    HTTP,
    HTTPS,
    MYSQL,
    POSTGRESQL,
    GRPC,
    ARROW_FLIGHT,
    INTERSERVER_HTTP,
    INTERSERVER_HTTPS,
    PROMETHEUS,

    // Keeper specific endpoints (exposed from clickhouse-server when Keeper is enabled)
    KEEPER_TCP,
    KEEPER_TCP_SECURE,
};

}



