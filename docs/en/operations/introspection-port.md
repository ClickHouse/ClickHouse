---
description: 'A native protocol port that serves queries while the server is starting up or shutting down, when the regular ports do not accept connections.'
sidebar_label: 'Introspection Port'
sidebar_position: 46
slug: /operations/introspection-port
title: 'Introspection Port'
doc_type: 'reference'
---

# Introspection port {#introspection-port}

When `clickhouse-server` is stuck in startup or in shutdown, the regular ports do not accept connections and the server cannot be asked what it is doing.

The introspection port is a native protocol TCP listener that starts before the server begins attaching tables and stops only after the tables' detach completes. During these windows an operator can connect to it with a stock `clickhouse-client` and run queries such as `SHOW PROCESSLIST`, `SELECT * FROM system.stack_trace`, or `SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5`.

## Configuration {#configuration}

The port is disabled by default. To enable it, define the `introspection` section in the server configuration:

```xml
<introspection>
    <tcp_port>9010</tcp_port>
    <tcp_port_secure>9011</tcp_port_secure>
    <listen_host>127.0.0.1</listen_host>
    <max_connections>4</max_connections>
</introspection>
```

| Key               | Description                                                                                                     |
|-------------------|-----------------------------------------------------------------------------------------------------------------|
| `tcp_port`        | Port for the native protocol, like the top-level `tcp_port`. Optional if `tcp_port_secure` is set.               |
| `tcp_port_secure` | Port for the native protocol over TLS, using the server certificate from the `openSSL.server` section. Optional. |
| `listen_host`     | Host(s) to bind. Defaults to the top-level `listen_host`.                                                         |
| `max_connections` | Size of the dedicated connection pool. Default: 4.                                                                |

## Behavior {#behavior}

- Queries on this port bypass `max_concurrent_queries` and the workload scheduler, like `SHOW PROCESSLIST` does; their concurrency is bounded by `max_connections`.
- Connections are served by a dedicated thread pool, so exhaustion of the regular connection pool does not affect this port.
- Full normal authentication and authorization apply. Connecting to this port grants nothing extra.
- The port is not subject to `SYSTEM START LISTEN` / `SYSTEM STOP LISTEN` (including `ALL`) and is not reconfigured on config reload.
