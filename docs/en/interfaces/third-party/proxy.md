---
sidebar_position: 29
sidebar_label: Proxies
---

# Proxy Servers from Third-party Developers {#proxy-servers-from-third-party-developers}

## chproxy {#chproxy}

[chproxy](https://github.com/Vertamedia/chproxy), is an HTTP proxy and load balancer for ClickHouse database.

Features:

-   Per-user routing and response caching.
-   Flexible limits.
-   Automatic SSL certificate renewal.

Implemented in Go.

## KittenHouse {#kittenhouse}

[KittenHouse](https://github.com/VKCOM/kittenhouse) is designed to be a local proxy between ClickHouse and application server in case it’s impossible or inconvenient to buffer INSERT data on your application side.

Features:

-   In-memory and on-disk data buffering.
-   Per-table routing.
-   Load-balancing and health checking.

Implemented in Go.

## ClickHouse-Bulk {#clickhouse-bulk}

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk) is a simple ClickHouse insert collector.

Features:

-   Group requests and send by threshold or interval.
-   Multiple remote servers.
-   Basic authentication.

Implemented in Go.

[Original article](https://clickhouse.com/docs/en/interfaces/third-party/proxy/) <!--hide-->
