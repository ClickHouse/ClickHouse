---
description: 'Overview of network interfaces, drivers, and tools for connecting to
  ClickHouse'
keywords: ['clickhouse', 'network', 'interfaces', 'http', 'tcp', 'grpc', 'command-line',
  'client', 'jdbc', 'odbc', 'driver']
sidebar_label: 'Overview'
slug: /interfaces/overview
title: 'Drivers and Interfaces'
doc_type: 'reference'
---

# Drivers and Interfaces

ClickHouse provides two network interfaces (they can be optionally wrapped in TLS for additional security):

- [HTTP](http.md), which is documented and easy to use directly.
- [Native TCP](../interfaces/tcp.md), which has less overhead.

In most cases it is recommended to use an appropriate tool or library instead of interacting with those directly. The following are officially supported by ClickHouse:

- [Command-line client](../interfaces/cli.md)
- [JDBC driver](../interfaces/jdbc.md)
- [ODBC driver](../interfaces/odbc.md)
- [C++ client library](../interfaces/cpp.md)

ClickHouse also supports two RPC protocols:
- [gRPC protocol](grpc.md) specially designed for ClickHouse.
- [Apache Arrow Flight](arrowflight.md).

ClickHouse server provides embedded visual interfaces for power users:

- Play UI: open `/play` in the browser;
- Advanced Dashboard: open `/dashboard` in the browser;
- Binary symbols viewer for ClickHouse engineers: open `/binary` in the browser;

There are also a wide range of third-party libraries for working with ClickHouse:

- [Client libraries](../interfaces/third-party/client-libraries.md)
- [Integrations](../interfaces/third-party/integrations.md)
- [Visual interfaces](../interfaces/third-party/gui.md)
